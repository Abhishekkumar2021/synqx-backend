from typing import Optional, Dict, Iterator, Callable
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app.models.pipelines import PipelineNode
from app.models.execution import PipelineRun
from app.models.enums import OperatorType, OperatorRunStatus
from app.core.logging import get_logger
from app.core.db_logging import DBLogger
from app.services.vault_service import VaultService
from app.connectors.factory import ConnectorFactory
from app.engine.transforms.factory import TransformFactory
from app.models.execution import Watermark
from app.models.connections import Asset, Connection
from app.core.errors import AppError
from app.engine.runner_core.state_manager import StateManager

logger = get_logger(__name__)

class NodeExecutor:
    def __init__(self, state_manager: StateManager):
        self.state_manager = state_manager

    def _fetch_asset_connection(self, db: Session, asset_id: int):
        asset = db.query(Asset).filter(Asset.id == asset_id).first()
        if not asset:
            raise AppError(f"Asset {asset_id} not found.")
        conn = db.query(Connection).filter(Connection.id == asset.connection_id).first()
        if not conn:
            raise AppError(f"Connection {asset.connection_id} not found.")
        return asset, conn

    def _materialize_iterator(self, data_iter: Iterator[pd.DataFrame]) -> list:
        return list(data_iter)

    def execute(
        self,
        pipeline_run: PipelineRun,
        node: PipelineNode,
        input_data: Optional[Dict[str, Callable[[], Iterator[pd.DataFrame]]]] = None
    ) -> Optional[Callable[[], Iterator[pd.DataFrame]]]:
        
        # Create Step Run
        step_run = self.state_manager.create_step_run(
            pipeline_run.id, node.id, node.operator_type, node.order_index
        )
        
        DBLogger.log_step(
            self.state_manager.db,
            step_run.id,
            "INFO",
            f"Starting node '{node.name}' ({node.operator_type.value})..."
        )

        attempts = 0
        max_attempts = (node.max_retries or 0) + 1

        while attempts < max_attempts:
            attempts += 1
            try:
                if attempts > 1:
                    DBLogger.log_step(
                        self.state_manager.db, 
                        step_run.id, 
                        "WARNING", 
                        f"Retrying node (attempt {attempts}/{max_attempts})"
                    )

                # --- Core Execution Logic ---
                result = self._execute_core(pipeline_run, node, step_run, input_data)
                return result

            except Exception as e:
                import time
                if attempts < max_attempts:
                    time.sleep(1)
                    continue
                
                # Final failure
                self.state_manager.update_step_status(
                    step_run, OperatorRunStatus.FAILED, 0, 0, 0, e
                )
                raise e

    def _execute_core(self, pipeline_run, node, step_run, input_data):
        db = self.state_manager.db
        
        # 1. Setup Connectors
        source_connector = None
        destination_connector = None
        source_asset = None
        dest_asset = None
        watermark = None

        if node.source_asset_id:
            source_asset, source_conn = self._fetch_asset_connection(db, node.source_asset_id)
            cfg = VaultService.get_connector_config(source_conn)
            source_connector = ConnectorFactory.get_connector(source_conn.connector_type.value, cfg)
            
            if source_asset.is_incremental_capable:
                watermark = db.query(Watermark).filter(
                    Watermark.pipeline_id == pipeline_run.pipeline_id,
                    Watermark.asset_id == source_asset.id
                ).first()

        if node.destination_asset_id:
            dest_asset, dest_conn = self._fetch_asset_connection(db, node.destination_asset_id)
            cfg = VaultService.get_connector_config(dest_conn)
            destination_connector = ConnectorFactory.get_connector(dest_conn.connector_type.value, cfg)

        is_sink = destination_connector is not None and dest_asset is not None

        # 2. Define Data Factory
        data_factory = None

        if source_connector and source_asset:
            def src_factory():
                records_in = 0
                bytes_in = 0
                try:
                    read_params = {"asset": source_asset.name}
                    if source_asset.config:
                        read_params.update(source_asset.config)
                    if watermark and watermark.last_value:
                        read_params["incremental_filter"] = watermark.last_value

                    with source_connector.session() as conn:
                        for chunk in conn.read_batch(**read_params):
                            records_in += len(chunk)
                            bytes_in += int(chunk.memory_usage(deep=True).sum())
                            yield chunk
                    
                    if not is_sink:
                        self.state_manager.update_step_status(
                            step_run, OperatorRunStatus.SUCCESS, records_in, records_in, bytes_in
                        )
                except Exception as e:
                    self.state_manager.update_step_status(
                        step_run, OperatorRunStatus.FAILED, records_in, 0, bytes_in, e
                    )
                    raise e
            
            data_factory = src_factory

        elif input_data:
            # Multi-Input
            if node.operator_type in {OperatorType.MERGE, OperatorType.UNION, OperatorType.JOIN}:
                def multi_input_factory():
                    records_in = 0
                    records_out = 0
                    bytes_out = 0
                    try:
                        input_iterators = {}
                        for uid, ufactory in input_data.items():
                            input_iterators[uid] = ufactory()
                        
                        # We need to wrap inputs to count IN records
                        counted_inputs = {}
                        def make_counter(iter_):
                            for chunk in iter_:
                                nonlocal records_in
                                records_in += len(chunk)
                                yield chunk

                        for uid, iter_ in input_iterators.items():
                            counted_inputs[uid] = make_counter(iter_)
                        
                        transform = TransformFactory.get_transform(node.operator_class, node.config)
                        output = transform.transform_multi(counted_inputs)
                        
                        for chunk in output:
                            records_out += len(chunk)
                            bytes_out += int(chunk.memory_usage(deep=True).sum())
                            yield chunk
                        
                        if not is_sink:
                            self.state_manager.update_step_status(
                                step_run, OperatorRunStatus.SUCCESS, records_in, records_out, bytes_out
                            )
                    except Exception as e:
                        self.state_manager.update_step_status(
                            step_run, OperatorRunStatus.FAILED, records_in, records_out, bytes_out, e
                        )
                        raise e
                
                data_factory = multi_input_factory
            
            # Single Input / Transform
            else:
                upstream_factory = next(iter(input_data.values()))
                
                def transform_factory():
                    stats = {"in": 0, "out": 0, "bytes": 0}
                    try:
                        upstream_iter = upstream_factory()
                        
                        def input_counter():
                            for chunk in upstream_iter:
                                stats["in"] += len(chunk)
                                yield chunk
                        
                        data_iter = input_counter()
                        transformed = data_iter
                        
                        if node.operator_type == OperatorType.TRANSFORM:
                            transform = TransformFactory.get_transform(node.operator_class, node.config)
                            transformed = transform.transform(data_iter)
                        
                        for chunk in transformed:
                            stats["out"] += len(chunk)
                            stats["bytes"] += int(chunk.memory_usage(deep=True).sum())
                            yield chunk
                        
                        if not is_sink:
                            self.state_manager.update_step_status(
                                step_run, OperatorRunStatus.SUCCESS, stats["in"], stats["out"], stats["bytes"]
                            )
                    except Exception as e:
                        self.state_manager.update_step_status(
                            step_run, OperatorRunStatus.FAILED, stats["in"], stats["out"], stats["bytes"], e
                        )
                        raise e
                
                data_factory = transform_factory
        else:
            def empty_factory():
                return iter([])
            data_factory = empty_factory

        # 3. Execution (Sink vs Intermediate)
        if is_sink:
            # SINK: Eager Execution
            data_iter = data_factory()
            sink_stats = {"in": 0, "bytes": 0}
            
            def sink_wrapper():
                for chunk in data_iter:
                    sink_stats["in"] += len(chunk)
                    sink_stats["bytes"] += int(chunk.memory_usage(deep=True).sum())
                    yield chunk
            
            with destination_connector.session() as conn:
                records_out = conn.write_batch(sink_wrapper(), asset=dest_asset.name)
            
            # Update Watermark
            if source_asset and source_asset.is_incremental_capable:
                if not watermark:
                    watermark = Watermark(
                        pipeline_id=pipeline_run.pipeline_id,
                        asset_id=source_asset.id,
                        watermark_type="timestamp"
                    )
                    db.add(watermark)
                
                col = "timestamp"
                if source_asset.config and "watermark_column" in source_asset.config:
                    col = source_asset.config["watermark_column"]
                
                watermark.last_value = {col: datetime.now(timezone.utc).isoformat()}
                watermark.last_updated = datetime.now(timezone.utc)
                db.add(watermark)

            self.state_manager.update_step_status(
                step_run, OperatorRunStatus.SUCCESS, sink_stats["in"], records_out, sink_stats["bytes"]
            )
            return None
        else:
            # INTERMEDIATE: Return Factory
            return data_factory
