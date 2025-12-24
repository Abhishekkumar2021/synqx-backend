from typing import Optional
from datetime import datetime, timezone
from sqlalchemy.orm import Session
from app.models.execution import StepRun, PipelineRun
from app.models.enums import OperatorRunStatus, PipelineRunStatus
from app.core.db_logging import DBLogger
from app.core.logging import get_logger

logger = get_logger(__name__)

class StateManager:
    """
    Manages the state of PipelineRuns and StepRuns, including DB persistence and logging.
    """
    def __init__(self, db: Session, job_id: int):
        self.db = db
        self.job_id = job_id

    def initialize_run(self, pipeline_id: int, version_id: int) -> PipelineRun:
        from sqlalchemy import func
        
        pipeline_run = self.db.query(PipelineRun).filter(PipelineRun.job_id == self.job_id).first()
        
        if pipeline_run:
            logger.info(f"Resuming/Retrying existing PipelineRun {pipeline_run.id} for Job {self.job_id}")
            pipeline_run.status = PipelineRunStatus.RUNNING
            pipeline_run.started_at = datetime.now(timezone.utc)
            pipeline_run.completed_at = None
            pipeline_run.error_message = None
        else:
            max_run = (
                self.db.query(func.max(PipelineRun.run_number))
                .filter(PipelineRun.pipeline_id == pipeline_id)
                .scalar()
            ) or 0
            
            pipeline_run = PipelineRun(
                job_id=self.job_id,
                pipeline_id=pipeline_id,
                pipeline_version_id=version_id,
                run_number=max_run + 1,
                status=PipelineRunStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
            self.db.add(pipeline_run)
        
        self.db.flush()
        self.db.refresh(pipeline_run)
        
        DBLogger.log_job(
            self.db,
            self.job_id,
            "INFO",
            f"PipelineRun {pipeline_run.run_number} started.",
            metadata={"pipeline_run_id": pipeline_run.id},
            source="runner",
        )
        return pipeline_run

    def create_step_run(self, pipeline_run_id: int, node_id: int, operator_type: str, order_index: int) -> StepRun:
        step_run = StepRun(
            pipeline_run_id=pipeline_run_id,
            node_id=node_id,
            operator_type=operator_type,
            order_index=order_index,
            status=OperatorRunStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )
        self.db.add(step_run)
        self.db.flush()
        return step_run

    def update_step_status(self, step_run: StepRun, status: OperatorRunStatus, records_in: int = 0, records_out: int = 0, bytes_processed: int = 0, error: Optional[Exception] = None):
        try:
            step_run.status = status
            step_run.completed_at = datetime.now(timezone.utc)
            if step_run.started_at:
                step_run.duration_seconds = (step_run.completed_at - step_run.started_at).total_seconds()
            
            step_run.records_in = records_in
            step_run.records_out = records_out
            step_run.bytes_processed = bytes_processed
            
            node_name = step_run.node.name if step_run.node else "Unknown Node"

            if error:
                step_run.error_message = str(error)
                step_run.error_type = type(error).__name__
                DBLogger.log_step(
                    self.db,
                    step_run.id,
                    "ERROR",
                    f"Node '{node_name}' failed: {error}",
                )
            else:
                DBLogger.log_step(
                    self.db,
                    step_run.id,
                    "SUCCESS",
                    f"Node '{node_name}' completed in {step_run.duration_seconds:.2f}s. Read: {records_in}, Written: {records_out}, Size: {bytes_processed} bytes.",
                )
            
            self.db.add(step_run)
            self.db.commit() # Commit immediately to persist status
        except Exception as e:
            logger.error(f"Failed to update step status: {e}")
            self.db.rollback()

    def fail_run(self, pipeline_run: PipelineRun, error: Exception):
        from app.models.enums import OperatorType
        
        # Reload to ensure we have the latest step runs
        self.db.refresh(pipeline_run)
        
        pipeline_run.status = PipelineRunStatus.FAILED
        pipeline_run.completed_at = datetime.now(timezone.utc)
        if pipeline_run.started_at:
            pipeline_run.duration_seconds = (pipeline_run.completed_at - pipeline_run.started_at).total_seconds()
        
        pipeline_run.error_message = str(error)
        
        # Aggregate stats even on failure to show partial progress
        pipeline_run.total_extracted = sum(step.records_in for step in pipeline_run.step_runs if step.operator_type == OperatorType.EXTRACT)
        pipeline_run.total_loaded = sum(step.records_out for step in pipeline_run.step_runs if step.operator_type == OperatorType.LOAD)
        pipeline_run.bytes_processed = sum(step.bytes_processed for step in pipeline_run.step_runs)

        self.db.add(pipeline_run)
        self.db.commit()

        DBLogger.log_job(
            self.db,
            self.job_id,
            "ERROR",
            f"PipelineRun {pipeline_run.run_number} failed: {error}",
            metadata={"pipeline_run_id": pipeline_run.id},
            source="runner",
        )

    def complete_run(self, pipeline_run: PipelineRun):
        from app.models.enums import OperatorType
        
        # Reload to ensure we have the latest step runs
        self.db.refresh(pipeline_run)
        
        pipeline_run.status = PipelineRunStatus.COMPLETED
        pipeline_run.completed_at = datetime.now(timezone.utc)
        
        # Aggregate stats
        pipeline_run.total_extracted = sum(step.records_in for step in pipeline_run.step_runs if step.operator_type == OperatorType.EXTRACT)
        pipeline_run.total_loaded = sum(step.records_out for step in pipeline_run.step_runs if step.operator_type == OperatorType.LOAD)
        pipeline_run.bytes_processed = sum(step.bytes_processed for step in pipeline_run.step_runs)
        
        if pipeline_run.started_at:
            pipeline_run.duration_seconds = (pipeline_run.completed_at - pipeline_run.started_at).total_seconds()
            
        self.db.add(pipeline_run)
        self.db.commit()

        DBLogger.log_job(
            self.db,
            self.job_id,
            "INFO",
            f"PipelineRun {pipeline_run.run_number} succeeded.",
            metadata={"pipeline_run_id": pipeline_run.id},
            source="runner",
        )
