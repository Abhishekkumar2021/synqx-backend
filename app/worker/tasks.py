from datetime import datetime, timezone
from app.core.celery_app import celery_app
from app.core.logging import get_logger
from app.db.session import session_scope
from app.models.execution import Job
from app.models.enums import JobStatus
from app.models.pipelines import PipelineVersion
from app.engine.runner import PipelineRunner
from app.core.db_logging import DBLogger

logger = get_logger(__name__)

@celery_app.task(name="app.worker.tasks.test_celery")
# ... (test_celery and db_check_task remain unchanged)

@celery_app.task(name="app.worker.tasks.execute_pipeline_task", bind=True)
def execute_pipeline_task(self, job_id: int) -> str:
    """
    Executes a pipeline run as a background task.
    """
    logger.info(f"execute_pipeline_task started for Job ID: {job_id}")
    
    try:
        with session_scope() as session:
            # 1. Retrieve the Job
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                logger.error(f"Job ID {job_id} not found.")
                return f"Job ID {job_id} not found."
            
            # Update job with celery task ID if not already set (it might be set by caller)
            if not job.celery_task_id:
                job.celery_task_id = self.request.id
            
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            session.commit()
            
            DBLogger.log_job(session, job.id, "INFO", "Job started.", source="worker", tenant_id=job.tenant_id)

            # 2. Retrieve Pipeline Version
            pipeline_version = session.query(PipelineVersion).filter(
                PipelineVersion.id == job.pipeline_version_id
            ).first()
            
            if not pipeline_version:
                error_msg = f"Pipeline Version ID {job.pipeline_version_id} not found."
                logger.error(error_msg)
                
                job.status = JobStatus.FAILED
                job.infra_error = error_msg
                job.completed_at = datetime.now(timezone.utc)
                DBLogger.log_job(session, job.id, "ERROR", error_msg, source="worker", tenant_id=job.tenant_id)
                session.commit()
                return error_msg

            # 3. Execute Pipeline
            runner = PipelineRunner()
            try:
                # The runner handles PipelineRun and StepRun creation/updates
                # It raises exceptions on failure
                runner.run(pipeline_version, session, job_id=job.id)
                
                # If we get here, the run was successful
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.now(timezone.utc)
                DBLogger.log_job(session, job.id, "INFO", "Job completed successfully.", source="worker", tenant_id=job.tenant_id)
                session.commit()
                logger.info(f"Job ID {job_id} completed successfully.")
                return f"Job ID {job_id} completed successfully."

            except Exception as e:
                logger.error(f"Pipeline execution failed for Job ID {job_id}: {e}", exc_info=True)
                job.status = JobStatus.FAILED
                job.infra_error = str(e)
                job.completed_at = datetime.now(timezone.utc)
                DBLogger.log_job(session, job.id, "ERROR", f"Pipeline execution failed: {e}", source="worker", tenant_id=job.tenant_id)
                session.commit()
                # Re-raise to ensure Celery marks the task as failed
                raise e

    except Exception as e:
        logger.error(f"Unexpected error in execute_pipeline_task for Job ID {job_id}: {e}", exc_info=True)
        raise e

@celery_app.task(name="app.worker.tasks.scheduler_heartbeat")
def scheduler_heartbeat() -> str:
    """
    Periodic task to check for scheduled pipelines.
    """
    from app.engine.scheduler import Scheduler # Import locally to avoid circular imports
    
    logger.info("scheduler_heartbeat started")
    try:
        with session_scope() as session:
            scheduler = Scheduler(session)
            scheduler.check_schedules()
            return "Scheduler heartbeat completed"
    except Exception as e:
        logger.error(f"Scheduler heartbeat failed: {e}", exc_info=True)
        raise e
