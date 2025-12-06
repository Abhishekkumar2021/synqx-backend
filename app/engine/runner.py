from typing import Optional, Iterator, Tuple, Dict, List
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from app.engine.dag import DAG, DagCycleError
from app.connectors.factory import ConnectorFactory
import app.connectors.impl # Ensure connectors are registered
from app.connectors.base import BaseConnector
from app.engine.transforms.factory import TransformFactory
from app.core.errors import AppError, ConfigurationError, DataTransferError
from app.models.pipelines import PipelineVersion, PipelineNode
from app.models.connections import Asset, Connection
from app.models.execution import PipelineRun, StepRun
from app.models.enums import PipelineRunStatus, OperatorRunStatus, OperatorType
from app.services.vault_service import VaultService
from app.core.db_logging import DBLogger
from app.core.logging import get_logger

logger = get_logger(__name__)

class PipelineRunner:
    """
    Executes a specific version of a data pipeline.
    It builds a DAG from the pipeline definition and executes nodes in topological order.
    """
    def __init__(self):
        pass

    def _build_dag(self, pipeline_version: PipelineVersion) -> DAG:
        """
        Constructs a DAG object from a PipelineVersion's nodes and edges.
        """
        dag = DAG()
        node_map: Dict[int, PipelineNode] = {node.id: node for node in pipeline_version.nodes}
        
        for node in pipeline_version.nodes:
            dag.add_node(node.node_id) # Use node_id (string) for DAG nodes

        for edge in pipeline_version.edges:
            from_node_obj = node_map.get(edge.from_node_id)
            to_node_obj = node_map.get(edge.to_node_id)
            
            if not from_node_obj or not to_node_obj:
                raise ConfigurationError(f"Edge refers to non-existent node IDs: {edge.from_node_id} -> {edge.to_node_id}")

            try:
                dag.add_edge(from_node_obj.node_id, to_node_obj.node_id)
            except ValueError as e:
                raise ConfigurationError(f"Invalid edge in pipeline version {pipeline_version.id}: {e}") from e

        try:
            # Check for cycles during DAG construction
            dag.topological_sort() 
        except DagCycleError as e:
            raise ConfigurationError(f"Pipeline version {pipeline_version.id} contains a cycle: {e}") from e

        return dag

    def _get_asset_and_connection(self, db_session: Session, asset_id: int) -> Tuple[Asset, Connection]:
        """Helper to fetch Asset and its Connection from the database."""
        asset = db_session.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            raise AppError(f"Asset with ID {asset_id} not found.")
        
        connection = db_session.query(Connection).filter(Connection.id == asset.connection_id).first()
        if not connection:
            raise AppError(f"Connection with ID {asset.connection_id} not found for asset {asset_id}.")
        
        return asset, connection

    def _execute_node(self, pipeline_run: PipelineRun, node: PipelineNode, db_session: Session, input_data: Optional[Iterator[pd.DataFrame]] = None) -> Optional[Iterator[pd.DataFrame]]:
        """
        Executes a single pipeline node and tracks its execution status.
        Returns the data iterator if the node produces data but doesn't consume it (e.g., Extract or Transform without Destination).
        """
        logger.info(f"Executing node: {node.name} (ID: {node.node_id}, Type: {node.operator_type})")
        
        step_run = StepRun(
            pipeline_run_id=pipeline_run.id,
            node_id=node.id,
            operator_type=node.operator_type,
            order_index=node.order_index,
            status=OperatorRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            tenant_id=pipeline_run.tenant_id
        )
        db_session.add(step_run)
        db_session.flush() # Flush to get step_run.id

        DBLogger.log_step(db_session, step_run.id, "INFO", f"Started execution of node '{node.name}'.", tenant_id=pipeline_run.tenant_id)

        source_connector: Optional[BaseConnector] = None
        destination_connector: Optional[BaseConnector] = None
        
        try:
            # --- Handle Source Connector (if exists) ---
            source_asset: Optional[Asset] = None
            source_connection: Optional[Connection] = None
            if node.source_asset_id:
                source_asset, source_connection = self._get_asset_and_connection(db_session, node.source_asset_id)
                try:
                    source_connector_config = VaultService.get_connector_config(source_connection)
                    source_connector = ConnectorFactory.get_connector(source_connection.connector_type.value, source_connector_config)
                    logger.debug(f"Source connector of type {source_connection.connector_type.value} instantiated for asset {source_asset.name}.")
                except ConfigurationError as e:
                    DBLogger.log_step(db_session, step_run.id, "ERROR", f"Source connector config error: {e}", tenant_id=pipeline_run.tenant_id)
                    raise ConfigurationError(f"Failed to get source connector for node {node.node_id}: {e}") from e
            
            # --- Handle Destination Connector (if exists) ---
            destination_asset: Optional[Asset] = None
            destination_connection: Optional[Connection] = None
            if node.destination_asset_id:
                destination_asset, destination_connection = self._get_asset_and_connection(db_session, node.destination_asset_id)
                try:
                    destination_connector_config = VaultService.get_connector_config(destination_connection)
                    destination_connector = ConnectorFactory.get_connector(destination_connection.connector_type.value, destination_connector_config)
                    logger.debug(f"Destination connector of type {destination_connection.connector_type.value} instantiated for asset {destination_asset.name}.")
                except ConfigurationError as e:
                    DBLogger.log_step(db_session, step_run.id, "ERROR", f"Destination connector config error: {e}", tenant_id=pipeline_run.tenant_id)
                    raise ConfigurationError(f"Failed to get destination connector for node {node.node_id}: {e}") from e

            # --- Data Transfer Logic ---
            data_iterator: Iterator[pd.DataFrame] = iter([]) 
            records_read = 0
            
            if source_connector and source_asset:
                # 1. Read from configured source
                try:
                    # Using a generator to count records while yielding
                    def read_with_counting():
                        nonlocal records_read
                        with source_connector.session() as conn:
                            for chunk in conn.read_batch(asset=source_asset.name):
                                records_read += len(chunk)
                                yield chunk
                    
                    data_iterator = read_with_counting()
                    logger.info(f"Started reading from source asset '{source_asset.name}' for node {node.node_id}.")
                    DBLogger.log_step(db_session, step_run.id, "INFO", f"Reading from source asset '{source_asset.name}'.", tenant_id=pipeline_run.tenant_id)
                except Exception as e:
                    DBLogger.log_step(db_session, step_run.id, "ERROR", f"Error reading source: {e}", tenant_id=pipeline_run.tenant_id)
                    raise DataTransferError(f"Error reading from source asset '{source_asset.name}' for node {node.node_id}: {e}") from e
            elif input_data is not None:
                # 2. Use input data from upstream
                # We wrap it to count records
                def read_from_input():
                    nonlocal records_read
                    for chunk in input_data:
                        records_read += len(chunk)
                        yield chunk
                data_iterator = read_from_input()
                logger.info(f"Using input data from upstream for node {node.node_id}.")
            
            # --- Transformation ---
            transformed_data_iterator = data_iterator
            if node.operator_type == OperatorType.TRANSFORM:
                try:
                    transform = TransformFactory.get_transform(node.operator_class, node.config)
                    transformed_data_iterator = transform.transform(data_iterator)
                    logger.info(f"Applied transform '{node.operator_class}' for node {node.node_id}.")
                    DBLogger.log_step(db_session, step_run.id, "INFO", f"Applied transform '{node.operator_class}'.", tenant_id=pipeline_run.tenant_id)
                except Exception as e:
                    DBLogger.log_step(db_session, step_run.id, "ERROR", f"Transform failed: {e}", tenant_id=pipeline_run.tenant_id)
                    raise ConfigurationError(f"Failed to apply transform for node {node.node_id}: {e}") from e

            # --- Write to Destination ---
            records_written = 0
            if destination_connector and destination_asset:
                try:
                    with destination_connector.session() as conn:
                        records_written = conn.write_batch(data=transformed_data_iterator, asset=destination_asset.name)
                    logger.info(f"Finished writing {records_written} records to destination asset '{destination_asset.name}' for node {node.node_id}.")
                    DBLogger.log_step(db_session, step_run.id, "INFO", f"Wrote {records_written} records to '{destination_asset.name}'.", tenant_id=pipeline_run.tenant_id)
                    
                    # Since data is consumed, we return None or empty
                    transformed_data_iterator = None 
                except Exception as e:
                    DBLogger.log_step(db_session, step_run.id, "ERROR", f"Error writing destination: {e}", tenant_id=pipeline_run.tenant_id)
                    raise DataTransferError(f"Error writing to destination asset '{destination_asset.name}' for node {node.node_id}: {e}") from e
            
            # If explicit consume required (e.g. transform but no destination and not returning)
            # But here we return the iterator if it wasn't consumed by destination.
            # The only case we implicitly consume is if it's a terminal node without write? 
            # No, if it returns, the next node or the system will handle it.
            # If this is the end of the DAG and no destination, the iterator won't be consumed, so Transforms won't run.
            # That's acceptable for lazy evaluation.

            step_run.records_in = records_read 
            step_run.records_out = records_written

            # Mark step run as successful
            step_run.status = OperatorRunStatus.SUCCESS
            step_run.completed_at = datetime.now(timezone.utc)
            if step_run.started_at:
                step_run.duration_seconds = (step_run.completed_at - step_run.started_at).total_seconds()
            db_session.add(step_run) # Re-add to ensure updates are tracked
            
            logger.info(f"Node {node.node_id} executed successfully. Records in: {step_run.records_in}, Records out: {step_run.records_out}")
            DBLogger.log_step(db_session, step_run.id, "INFO", f"Step completed. In: {records_read}, Out: {records_written}", tenant_id=pipeline_run.tenant_id)
            
            return transformed_data_iterator

        except Exception as e:
            # ... (error handling remains same)
            step_run.status = OperatorRunStatus.FAILED
            step_run.completed_at = datetime.now(timezone.utc)
            if step_run.started_at:
                step_run.duration_seconds = (step_run.completed_at - step_run.started_at).total_seconds()
            step_run.error_message = str(e)
            step_run.error_type = type(e).__name__
            db_session.add(step_run) # Re-add to ensure updates are tracked
            DBLogger.log_step(db_session, step_run.id, "ERROR", f"Step failed: {e}", tenant_id=pipeline_run.tenant_id)
            logger.error(f"Node {node.node_id} failed: {e}", exc_info=True)
            raise # Re-raise to propagate the error

    def run(self, pipeline_version: PipelineVersion, db_session: Session, job_id: int) -> None:
        """
        Executes a pipeline based on its version definition and tracks its execution.
        """
        logger.info(f"Starting execution for pipeline version {pipeline_version.id} (Pipeline: {pipeline_version.pipeline_id}, Job: {job_id}).")
        
        # Determine the next run number
        from sqlalchemy import func
        max_run = db_session.query(func.max(PipelineRun.run_number)).filter(
            PipelineRun.pipeline_id == pipeline_version.pipeline_id
        ).scalar()
        next_run_number = (max_run or 0) + 1

        # Create PipelineRun record
        pipeline_run = PipelineRun(
            job_id=job_id,
            pipeline_id=pipeline_version.pipeline_id,
            pipeline_version_id=pipeline_version.id,
            run_number=next_run_number,
            status=PipelineRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
            tenant_id=pipeline_version.tenant_id
        )
        db_session.add(pipeline_run)
        db_session.flush() # Flush to get pipeline_run.id
        
        # We could log to JobLog here as well, linking PipelineRun ID in metadata
        DBLogger.log_job(db_session, job_id, "INFO", f"PipelineRun {next_run_number} started.", metadata={"pipeline_run_id": pipeline_run.id}, source="runner", tenant_id=pipeline_run.tenant_id)

        try:
            dag = self._build_dag(pipeline_version)
            execution_order = dag.topological_sort()
            logger.info(f"Execution order: {execution_order}")

            node_id_to_obj_map = {node.node_id: node for node in pipeline_version.nodes}
            node_outputs: Dict[str, Iterator[pd.DataFrame]] = {}

            for node_id in execution_order:
                node_obj = node_id_to_obj_map.get(node_id)
                if not node_obj:
                    raise AppError(f"Node '{node_id}' not found in pipeline_version nodes list.")
                
                # Determine inputs
                input_data = None
                upstream_nodes = dag.get_upstream_nodes(node_id)
                if upstream_nodes:
                    # For simplicity, take the first upstream output. 
                    # In complex DAGs, we might need to merge multiple inputs.
                    # Since iterators are consumed once, this only supports linear consumption or simple fan-in (merge).
                    first_upstream = list(upstream_nodes)[0] 
                    input_data = node_outputs.get(first_upstream)
                
                output = self._execute_node(pipeline_run, node_obj, db_session, input_data=input_data)
                
                if output is not None:
                    node_outputs[node_id] = output
            
            # If all nodes succeed
            pipeline_run.status = PipelineRunStatus.COMPLETED
            pipeline_run.completed_at = datetime.now(timezone.utc)
            if pipeline_run.started_at:
                pipeline_run.duration_seconds = (pipeline_run.completed_at - pipeline_run.started_at).total_seconds()
            db_session.add(pipeline_run)
            db_session.commit() # Commit changes to DB
            
            logger.info(f"Pipeline version {pipeline_version.id} executed successfully.")
            DBLogger.log_job(db_session, job_id, "INFO", f"PipelineRun {next_run_number} finished successfully.", metadata={"pipeline_run_id": pipeline_run.id}, source="runner", tenant_id=pipeline_run.tenant_id)

        except Exception as e:
            db_session.rollback() # Rollback any uncommitted changes from this run
            pipeline_run.status = PipelineRunStatus.FAILED
            pipeline_run.completed_at = datetime.now(timezone.utc)
            if pipeline_run.started_at:
                pipeline_run.duration_seconds = (pipeline_run.completed_at - pipeline_run.started_at).total_seconds()
            pipeline_run.error_message = str(e)
            db_session.add(pipeline_run) # Re-add to ensure updates are tracked
            db_session.commit() # Commit failure status
            
            logger.error(f"Pipeline version {pipeline_version.id} failed: {e}", exc_info=True)
            # We rely on the worker to log the final Job failure, but we can log the runner's perspective
            DBLogger.log_job(db_session, job_id, "ERROR", f"PipelineRun {next_run_number} failed: {e}", metadata={"pipeline_run_id": pipeline_run.id}, source="runner", tenant_id=pipeline_run.tenant_id)
            raise # Re-raise to propagate the error