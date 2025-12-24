from typing import Dict, Callable, Iterator, Optional
import pandas as pd
from sqlalchemy.orm import Session
from collections import defaultdict
import concurrent.futures

from app.engine.dag import DAG, DagCycleError
from app.models.pipelines import PipelineVersion
from app.core.errors import ConfigurationError
from app.core.logging import get_logger
from app.engine.runner_core.state_manager import StateManager
from app.engine.runner_core.node_executor import NodeExecutor

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
        return dag

    def run(self, pipeline_version: PipelineVersion, db: Session, job_id: int) -> None:
        state_manager = StateManager(db, job_id)
        node_executor = NodeExecutor(state_manager)
        
        pipeline_run = state_manager.initialize_run(pipeline_version.pipeline_id, pipeline_version.id)
        
        try:
            dag = self._build_dag(pipeline_version)
            order = dag.topological_sort()
            node_map = {n.node_id: n for n in pipeline_version.nodes}
            
            # Stores factories: NodeID -> Factory
            outputs: Dict[str, Callable[[], Iterator[pd.DataFrame]]] = defaultdict(lambda: (lambda: iter([])))

            # Phase 1: Setup & Execution
            # We execute in topological order. 
            # Non-sink nodes return factories immediately.
            # Sink nodes execute eagerly.
            
            # To support parallelism for sinks, we could collect sink tasks.
            # For now, we keep it sequential for safety and simplicity in the refactor.
            
            for node_id in order:
                node = node_map[node_id]
                upstream_nodes = dag.get_upstream_nodes(node_id)
                
                input_data = None
                if upstream_nodes:
                    input_data = {
                        uid: outputs[uid] for uid in upstream_nodes
                    }
                
                result = node_executor.execute(pipeline_run, node, input_data)
                
                if result is not None:
                    outputs[node_id] = result

            state_manager.complete_run(pipeline_run)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            state_manager.fail_run(pipeline_run, e)
            raise e
