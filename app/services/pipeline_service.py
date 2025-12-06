from typing import List, Optional, Dict, Any
import uuid
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.models.pipelines import Pipeline, PipelineVersion, PipelineNode, PipelineEdge
from app.models.execution import Job
from app.models.enums import PipelineStatus, JobStatus
from app.schemas.pipeline import PipelineCreate, PipelineVersionCreate, PipelineNodeCreate, PipelineEdgeCreate
from app.engine.runner import PipelineRunner 
from app.core.errors import AppError, ConfigurationError
from app.core.logging import get_logger

logger = get_logger(__name__)

class PipelineService:
    def __init__(self, db_session: Session):
        self.db_session = db_session
        self.pipeline_runner = PipelineRunner() 

    def create_pipeline(self, pipeline_create: PipelineCreate, tenant_id: int) -> Pipeline:
        """
        Creates a new pipeline along with its initial version, nodes, and edges.
        """
        try:
            # Ensure tenant_id is string
            tenant_id_str = str(tenant_id)

            # Create the main Pipeline object
            db_pipeline = Pipeline(
                name=pipeline_create.name,
                description=pipeline_create.description,
                schedule_cron=pipeline_create.schedule_cron,
                schedule_enabled=pipeline_create.schedule_enabled,
                schedule_timezone=pipeline_create.schedule_timezone,
                max_parallel_runs=pipeline_create.max_parallel_runs,
                execution_timeout_seconds=pipeline_create.execution_timeout_seconds,
                tags=pipeline_create.tags,
                priority=pipeline_create.priority,
                tenant_id=tenant_id_str 
            )
            self.db_session.add(db_pipeline)
            self.db_session.flush() 

            # Create the initial PipelineVersion
            db_version = self._create_pipeline_version(
                db_pipeline.id, 
                pipeline_create.initial_version, 
                version_number=1,
                is_published=True,
                tenant_id=tenant_id_str
            )
            self.db_session.add(db_version)
            self.db_session.flush() 

            # Update the pipeline with its current and published version
            db_pipeline.current_version = db_version.version
            db_pipeline.published_version_id = db_version.id
            db_pipeline.status = PipelineStatus.ACTIVE

            # Create Nodes for the version
            self._create_pipeline_nodes(db_version.id, pipeline_create.initial_version.nodes, tenant_id=tenant_id_str)

            # Create Edges for the version
            self._create_pipeline_edges(db_version.id, pipeline_create.initial_version.edges, tenant_id=tenant_id_str)

            self.db_session.commit()
            self.db_session.refresh(db_pipeline)
            logger.info(f"Pipeline '{db_pipeline.name}' (ID: {db_pipeline.id}) created successfully with initial version.")
            return db_pipeline

        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error creating pipeline: {e}")
            raise AppError(f"Failed to create pipeline due to a database error: {e}") from e
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"An unexpected error occurred while creating pipeline: {e}")
            raise AppError(f"Failed to create pipeline: {e}") from e

    def get_pipeline(self, pipeline_id: int, tenant_id: int) -> Optional[Pipeline]:
        """
        Retrieves a pipeline by its ID.
        """
        pipeline = self.db_session.query(Pipeline).filter(
            Pipeline.id == pipeline_id,
            Pipeline.tenant_id == str(tenant_id),
            Pipeline.deleted_at.is_(None)
        ).first()
        return pipeline

    def get_pipeline_version(self, pipeline_id: int, version_id: Optional[int] = None, tenant_id: int = None) -> Optional[PipelineVersion]:
        """
        Retrieves a specific pipeline version or the currently published one.
        """
        query = self.db_session.query(PipelineVersion).join(Pipeline).filter(
            Pipeline.id == pipeline_id,
            Pipeline.tenant_id == str(tenant_id),
            Pipeline.deleted_at.is_(None)
        )
        if version_id:
            query = query.filter(PipelineVersion.id == version_id)
        else:
            query = query.filter(PipelineVersion.is_published == True) 

        version = query.first()
        return version

    def trigger_pipeline_run(self, pipeline_id: int, tenant_id: int, version_id: Optional[int] = None) -> Dict[str, Any]:
        """
        Triggers a run for a specified pipeline version.
        Executes synchronously for now (wrapper around job creation + run).
        """
        pipeline_version = self.get_pipeline_version(pipeline_id, version_id, tenant_id)
        if not pipeline_version:
            raise AppError(f"Pipeline version not found for pipeline ID {pipeline_id}, version ID {version_id}.")

        # Eager load nodes and edges for the runner
        pipeline_version_with_details = self.db_session.query(PipelineVersion).filter(
            PipelineVersion.id == pipeline_version.id
        ).first()

        if not pipeline_version_with_details:
             raise AppError(f"Could not load pipeline version details for pipeline ID {pipeline_id}, version ID {version_id}.")

        # Create a Job record to track this execution
        job = Job(
            pipeline_id=pipeline_id,
            pipeline_version_id=pipeline_version.id,
            correlation_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            tenant_id=str(tenant_id)
        )
        self.db_session.add(job)
        self.db_session.flush() # Get job ID

        try:
            # Update Job status to RUNNING
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            self.db_session.commit()

            # Execute the pipeline
            self.pipeline_runner.run(pipeline_version_with_details, db_session=self.db_session, job_id=job.id)
            
            # Update Job status to COMPLETED
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now(timezone.utc)
            self.db_session.add(job)
            self.db_session.commit()

            logger.info(f"Successfully triggered and ran pipeline {pipeline_id}, version {pipeline_version.version}, Job ID: {job.id}.")
            return {"status": "success", "message": "Pipeline run completed successfully.", "job_id": job.id}
            
        except Exception as e:
            # Update Job status to FAILED
            logger.error(f"Failed to run pipeline {pipeline_id}, version {pipeline_version.version}: {e}")
            self.db_session.rollback()
            
            failed_job = self.db_session.query(Job).filter(Job.id == job.id).first()
            if failed_job:
                failed_job.status = JobStatus.FAILED
                failed_job.completed_at = datetime.now(timezone.utc)
                failed_job.infra_error = str(e)
                self.db_session.add(failed_job)
                self.db_session.commit()
            
            raise AppError(f"Failed to run pipeline: {e}") from e

    def _create_pipeline_version(
        self, 
        pipeline_id: int, 
        version_data: PipelineVersionCreate, 
        version_number: int,
        is_published: bool,
        tenant_id: str
    ) -> PipelineVersion:
        """Helper to create a PipelineVersion object."""
        db_version = PipelineVersion(
            pipeline_id=pipeline_id,
            version=version_number,
            config_snapshot=version_data.config_snapshot,
            change_summary=version_data.change_summary,
            version_notes=version_data.version_notes,
            is_published=is_published,
            published_at=datetime.now(timezone.utc) if is_published else None,
            tenant_id=tenant_id
        )
        return db_version

    def _create_pipeline_nodes(self, pipeline_version_id: int, nodes_data: List[PipelineNodeCreate], tenant_id: str) -> None:
        """Helper to create PipelineNode objects."""
        for node_data in nodes_data:
            db_node = PipelineNode(
                pipeline_version_id=pipeline_version_id,
                node_id=node_data.node_id,
                name=node_data.name,
                description=node_data.description,
                operator_type=node_data.operator_type,
                operator_class=node_data.operator_class,
                config=node_data.config,
                order_index=node_data.order_index,
                source_asset_id=node_data.source_asset_id,
                destination_asset_id=node_data.destination_asset_id,
                max_retries=node_data.max_retries,
                timeout_seconds=node_data.timeout_seconds,
                tenant_id=tenant_id
            )
            self.db_session.add(db_node)

    def _create_pipeline_edges(self, pipeline_version_id: int, edges_data: List[PipelineEdgeCreate], tenant_id: str) -> None:
        """Helper to create PipelineEdge objects."""
        for edge_data in edges_data:
            db_edge = PipelineEdge(
                pipeline_version_id=pipeline_version_id,
                from_node_id=self._get_node_db_id(pipeline_version_id, edge_data.from_node_id),
                to_node_id=self._get_node_db_id(pipeline_version_id, edge_data.to_node_id),
                edge_type=edge_data.edge_type,
                tenant_id=tenant_id
            )
            self.db_session.add(db_edge)
            
    def _get_node_db_id(self, pipeline_version_id: int, node_code_id: str) -> int:
        """
        Helper to get the database ID of a node given its pipeline_version_id and node_id (code).
        This assumes nodes are flushed or committed before edges are created.
        """
        node = self.db_session.query(PipelineNode).filter(
            PipelineNode.pipeline_version_id == pipeline_version_id,
            PipelineNode.node_id == node_code_id
        ).first()
        if not node:
            raise ConfigurationError(f"Node with code ID '{node_code_id}' not found for version {pipeline_version_id}. "
                                     "Ensure nodes are defined before edges referencing them.")
        return node.id
