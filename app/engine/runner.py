from typing import Optional, Iterator, Dict, List, Tuple, Any
import pandas as pd
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from collections import defaultdict

from app.engine.dag import DAG, DagCycleError
from app.connectors.factory import ConnectorFactory
import app.connectors.impl
from app.engine.transforms.factory import TransformFactory
from app.core.errors import AppError, ConfigurationError
from app.models.pipelines import PipelineVersion, PipelineNode
from app.models.connections import Asset, Connection
from app.models.execution import PipelineRun, StepRun
from app.models.enums import PipelineRunStatus, OperatorRunStatus, OperatorType
from app.services.vault_service import VaultService
from app.core.db_logging import DBLogger
from app.core.logging import get_logger

logger = get_logger(__name__)


class PipelineRunner:
    def __init__(self):
        pass

    def _build_dag(self, pipeline_version: PipelineVersion) -> DAG:
        dag = DAG()
        node_map = {node.id: node for node in pipeline_version.nodes}
        for node in pipeline_version.nodes:
            dag.add_node(node.node_id)
        for edge in pipeline_version.edges:
            f, t = node_map.get(edge.from_node_id), node_map.get(edge.to_node_id)
            if not f or not t or f.node_id == t.node_id:
                raise ConfigurationError(
                    f"Invalid edge: {edge.from_node_id}->{edge.to_node_id}"
                )
            dag.add_edge(f.node_id, t.node_id)
        try:
            dag.topological_sort()
        except DagCycleError as e:
            raise ConfigurationError(
                f"Pipeline {pipeline_version.id} contains a cycle: {e}"
            )

        # Validate DAG semantics
        self._validate_dag_semantics(dag, pipeline_version)
        return dag

    def _fetch_asset_connection(
        self, db: Session, asset_id: int
    ) -> Tuple[Asset, Connection]:
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            raise AppError(f"Asset {asset_id} not found.")
        conn = db.query(Connection).filter(Connection.id == asset.connection_id).first()
        if not conn:
            raise AppError(
                f"Connection {asset.connection_id} not found for asset {asset_id}."
            )
        return asset, conn

    def _materialize_iterator(
        self, data_iter: Iterator[pd.DataFrame]
    ) -> List[pd.DataFrame]:
        """Materialize an iterator into a list of DataFrames for multi-input operators."""
        return list(data_iter)

    def _execute_node(
        self,
        pipeline_run: PipelineRun,
        node: PipelineNode,
        db: Session,
        input_data: Optional[Dict[str, Iterator[pd.DataFrame]]] = None,
    ) -> Optional[Iterator[pd.DataFrame]]:
        """
        Execute a single node in the pipeline.

        Args:
            pipeline_run: The current pipeline run
            node: The node to execute
            db: Database session
            input_data: Dictionary mapping upstream node_ids to their output iterators

        Returns:
            Iterator of DataFrames if node produces output, None otherwise
        """
        logger.info(
            "Executing node", node=node.node_id, operator=node.operator_type.value
        )

        step_run = StepRun(
            pipeline_run_id=pipeline_run.id,
            node_id=node.id,
            operator_type=node.operator_type,
            order_index=node.order_index,
            status=OperatorRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        db.add(step_run)
        db.flush()

        DBLogger.log_step(
            db,
            step_run.id,
            "INFO",
            f"Node '{node.name}' started.",
        )

        try:
            source_connector = None
            destination_connector = None
            source_asset = None
            dest_asset = None

            # Setup source connector if needed
            if node.source_asset_id:
                source_asset, source_conn = self._fetch_asset_connection(
                    db, node.source_asset_id
                )
                cfg = VaultService.get_connector_config(source_conn)
                source_connector = ConnectorFactory.get_connector(
                    source_conn.connector_type.value, cfg
                )

            # Setup destination connector if needed
            if node.destination_asset_id:
                dest_asset, dest_conn = self._fetch_asset_connection(
                    db, node.destination_asset_id
                )
                cfg = VaultService.get_connector_config(dest_conn)
                destination_connector = ConnectorFactory.get_connector(
                    dest_conn.connector_type.value, cfg
                )

            records_in = 0
            records_out = 0

            # Determine data source
            if source_connector and source_asset:
                # Node reads from a source connector
                def src_iter():
                    nonlocal records_in
                    with source_connector.session() as conn:
                        for chunk in conn.read_batch(asset=source_asset.name):
                            records_in += len(chunk)
                            yield chunk

                data_iter = src_iter()

            elif input_data:
                # Node receives input from upstream nodes
                if node.operator_type in {
                    OperatorType.MERGE,
                    OperatorType.UNION,
                    OperatorType.JOIN,
                }:
                    # Multi-input operators need all upstream outputs
                    # Materialize all inputs for proper handling
                    materialized_inputs = {}
                    for upstream_id, upstream_iter in input_data.items():
                        materialized_inputs[upstream_id] = self._materialize_iterator(
                            upstream_iter
                        )
                        for chunk in materialized_inputs[upstream_id]:
                            records_in += len(chunk)

                    # Create transform with multiple inputs
                    transform = TransformFactory.get_transform(
                        node.operator_class, node.config
                    )
                    # Pass all inputs to the transform
                    data_iter = transform.transform_multi(materialized_inputs)

                else:
                    # Single-input operators: use first available upstream output
                    upstream_iter = next(iter(input_data.values()))

                    def upstream_iter_wrapper():
                        nonlocal records_in
                        for chunk in upstream_iter:
                            records_in += len(chunk)
                            yield chunk

                    data_iter = upstream_iter_wrapper()
            else:
                # No input data
                data_iter = iter([])

            # Apply transformation if needed
            transformed = data_iter
            if node.operator_type == OperatorType.TRANSFORM:
                transform = TransformFactory.get_transform(
                    node.operator_class, node.config
                )
                transformed = transform.transform(data_iter)

            # Write to destination if needed
            if destination_connector and dest_asset:
                with destination_connector.session() as conn:
                    records_out = conn.write_batch(
                        data=transformed, asset=dest_asset.name
                    )
                transformed = None  # Data consumed by destination
            else:
                # Count records for pass-through nodes
                iterator_to_count = transformed

                def counting_iter():
                    nonlocal records_out
                    for chunk in iterator_to_count:
                        records_out += len(chunk)
                        yield chunk

                if transformed is not None:
                    transformed = counting_iter()

            # Update step run with results
            step_run.records_in = records_in
            step_run.records_out = records_out
            step_run.status = OperatorRunStatus.SUCCESS
            step_run.completed_at = datetime.now(timezone.utc)
            if step_run.started_at:
                step_run.duration_seconds = (
                    step_run.completed_at - step_run.started_at
                ).total_seconds()

            db.add(step_run)

            logger.info(
                "Node completed",
                node=node.node_id,
                records_in=records_in,
                records_out=records_out,
            )
            DBLogger.log_step(
                db,
                step_run.id,
                "INFO",
                f"Completed. In={records_in}, Out={records_out}",
            )

            return transformed

        except Exception as e:
            step_run.status = OperatorRunStatus.FAILED
            step_run.completed_at = datetime.now(timezone.utc)
            if step_run.started_at:
                step_run.duration_seconds = (
                    step_run.completed_at - step_run.started_at
                ).total_seconds()
            step_run.error_message = str(e)
            step_run.error_type = type(e).__name__
            db.add(step_run)

            DBLogger.log_step(
                db,
                step_run.id,
                "ERROR",
                f"Step failed: {e}",
            )
            logger.error("Node failed", node=node.node_id, error=str(e))
            raise

    def run(self, pipeline_version: PipelineVersion, db: Session, job_id: int) -> None:
        logger.info(
            "Pipeline execution started",
            pipeline_version=pipeline_version.id,
            job_id=job_id,
        )

        from sqlalchemy import func

        # Check for existing run for this job (retry case)
        pipeline_run = (
            db.query(PipelineRun)
            .filter(PipelineRun.job_id == job_id)
            .first()
        )

        if pipeline_run:
            logger.info(f"Resuming/Retrying existing PipelineRun {pipeline_run.id} for Job {job_id}")
            pipeline_run.status = PipelineRunStatus.RUNNING
            pipeline_run.started_at = datetime.now(timezone.utc)
            pipeline_run.completed_at = None
            pipeline_run.error_message = None
            # run_number stays the same or we could increment if we tracked retries explicitly here
        else:
            max_run = (
                db.query(func.max(PipelineRun.run_number))
                .filter(PipelineRun.pipeline_id == pipeline_version.pipeline_id)
                .scalar()
            )
            next_run = (max_run or 0) + 1

            pipeline_run = PipelineRun(
                job_id=job_id,
                pipeline_id=pipeline_version.pipeline_id,
                pipeline_version_id=pipeline_version.id,
                run_number=next_run,
                status=PipelineRunStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
            db.add(pipeline_run)
        
        db.flush()

        DBLogger.log_job(
            db,
            job_id,
            "INFO",
            f"PipelineRun {pipeline_run.run_number} started.",
            metadata={"pipeline_run_id": pipeline_run.id},
            source="runner",
        )

        try:
            dag = self._build_dag(pipeline_version)
            order = dag.topological_sort()
            node_map = {n.node_id: n for n in pipeline_version.nodes}

            # Store outputs for each node
            outputs: Dict[str, Iterator[pd.DataFrame]] = {}

            # Execute nodes in topological order
            for node_id in order:
                node = node_map[node_id]
                upstream_nodes = dag.get_upstream_nodes(node_id)

                # Prepare input data from all upstream nodes
                input_data = None
                if upstream_nodes:
                    input_data = {
                        upstream_id: outputs[upstream_id]
                        for upstream_id in upstream_nodes
                        if upstream_id in outputs
                    }

                    # Validate that all expected inputs are available
                    if len(input_data) != len(upstream_nodes):
                        missing = upstream_nodes - set(input_data.keys())
                        raise AppError(
                            f"Node {node_id} missing inputs from upstream nodes: {missing}"
                        )

                # Execute the node
                out = self._execute_node(pipeline_run, node, db, input_data=input_data)

                # Store output if node produces data
                if out is not None:
                    outputs[node_id] = out

            # Mark pipeline as completed
            pipeline_run.status = PipelineRunStatus.COMPLETED
            pipeline_run.completed_at = datetime.now(timezone.utc)
            if pipeline_run.started_at:
                pipeline_run.duration_seconds = (
                    pipeline_run.completed_at - pipeline_run.started_at
                ).total_seconds()
            db.add(pipeline_run)
            db.commit()

            logger.info("Pipeline completed", pipeline_version=pipeline_version.id)
            DBLogger.log_job(
                db,
                job_id,
                "INFO",
                f"PipelineRun {next_run} succeeded.",
                metadata={"pipeline_run_id": pipeline_run.id},
                source="runner",
            )

        except Exception as e:
            db.rollback()
            pipeline_run.status = PipelineRunStatus.FAILED
            pipeline_run.completed_at = datetime.now(timezone.utc)
            if pipeline_run.started_at:
                pipeline_run.duration_seconds = (
                    pipeline_run.completed_at - pipeline_run.started_at
                ).total_seconds()
            pipeline_run.error_message = str(e)
            db.add(pipeline_run)
            db.commit()

            logger.error(
                "Pipeline failed", pipeline_version=pipeline_version.id, error=str(e)
            )
            DBLogger.log_job(
                db,
                job_id,
                "ERROR",
                f"PipelineRun {next_run} failed: {e}",
                metadata={"pipeline_run_id": pipeline_run.id},
                source="runner",
            )
            raise

    def _validate_dag_semantics(
        self, dag: DAG, pipeline_version: PipelineVersion
    ) -> None:
        """
        Validate DAG semantic rules:
        - Nodes with multiple parents must be merge/union/join operators
        - Source nodes should not have parents
        - Destination nodes should have parents
        """
        node_map = {n.node_id: n for n in pipeline_version.nodes}

        for node_id in dag.get_nodes():
            upstream = dag.get_upstream_nodes(node_id)
            downstream = dag.get_downstream_nodes(node_id)
            node = node_map[node_id]

            # Check multi-parent constraint
            if len(upstream) > 1:
                if node.operator_type not in {
                    OperatorType.MERGE,
                    OperatorType.UNION,
                    OperatorType.JOIN,
                }:
                    raise ConfigurationError(
                        f"Node '{node.node_id}' has multiple parents {list(upstream)} "
                        f"but operator type '{node.operator_type.value}' does not support multiple inputs."
                    )

            # Validate source nodes (nodes that read from external sources)
            if node.source_asset_id and len(upstream) > 0:
                logger.warning(
                    f"Node '{node.node_id}' has source asset but also has upstream nodes. "
                    f"Upstream data will be ignored."
                )

            # Validate leaf nodes (nodes with no downstream)
            if len(downstream) == 0 and not node.destination_asset_id:
                logger.warning(
                    f"Node '{node.node_id}' has no destination asset and no downstream nodes. "
                    f"Its output will be discarded."
                )
