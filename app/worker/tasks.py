from datetime import datetime, timezone
from typing import Optional
from celery.exceptions import SoftTimeLimitExceeded, Retry
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
def test_celery():
    """Health check task for Celery workers."""
    return "Celery OK"


@celery_app.task(
    name="app.worker.tasks.execute_pipeline_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # 1 minute
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,  # 10 minutes max
    retry_jitter=True,
    acks_late=True,  # Task acknowledged after completion
    reject_on_worker_lost=True,
    time_limit=3600,  # 1 hour hard limit
    soft_time_limit=3300,  # 55 minute soft limit
)
def execute_pipeline_task(self, job_id: int) -> str:
    """
    Execute a pipeline job.

    Args:
        job_id: The ID of the job to execute

    Returns:
        Success message

    Raises:
        Exception: If job execution fails after all retries
    """
    logger.info(
        "Pipeline execution task started",
        extra={
            "job_id": job_id,
            "task_id": self.request.id,
            "retries": self.request.retries,
        },
    )

    try:
        with session_scope() as session:
            # Fetch and validate job
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                error_msg = f"Job ID {job_id} not found"
                logger.error(error_msg, extra={"job_id": job_id})
                return error_msg

            # Update job with task ID if not set
            if not job.celery_task_id:
                job.celery_task_id = self.request.id
                session.commit()

            # Check if job is already completed or running
            if job.status in [JobStatus.SUCCESS, JobStatus.RUNNING]:
                logger.warning(
                    f"Job {job_id} already in status {job.status.value}",
                    extra={"job_id": job_id, "status": job.status.value},
                )
                return f"Job {job_id} already {job.status.value}"

            # Mark job as running
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now(timezone.utc)
            job.retry_count = self.request.retries
            session.commit()

            DBLogger.log_job(
                session,
                job.id,
                "INFO",
                f"Job started (attempt {self.request.retries + 1})",
                source="worker",
                tenant_id=job.tenant_id,
            )

            # Fetch pipeline version
            pipeline_version = (
                session.query(PipelineVersion)
                .filter(PipelineVersion.id == job.pipeline_version_id)
                .first()
            )

            if not pipeline_version:
                error_msg = f"Pipeline Version ID {job.pipeline_version_id} not found"
                logger.error(error_msg, extra={"job_id": job_id})
                _mark_job_failed(session, job, error_msg, is_infra_error=True)
                return error_msg

            # Validate pipeline version has nodes
            if not pipeline_version.nodes:
                error_msg = "Pipeline version has no nodes"
                logger.error(error_msg, extra={"job_id": job_id})
                _mark_job_failed(session, job, error_msg, is_infra_error=True)
                return error_msg

            # Execute pipeline
            runner = PipelineRunner()

            try:
                logger.info(
                    "Starting pipeline execution",
                    extra={
                        "job_id": job_id,
                        "pipeline_version_id": pipeline_version.id,
                        "node_count": len(pipeline_version.nodes),
                    },
                )

                runner.run(pipeline_version, session, job_id=job.id)

                # Mark job as successful
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.now(timezone.utc)

                if job.started_at:
                    job.duration_seconds = (
                        job.completed_at - job.started_at
                    ).total_seconds()

                session.commit()

                DBLogger.log_job(
                    session,
                    job.id,
                    "INFO",
                    f"Job completed successfully in {job.duration_seconds:.2f}s",
                    source="worker",
                    tenant_id=job.tenant_id,
                )

                logger.info(
                    "Pipeline execution completed",
                    extra={"job_id": job_id, "duration_seconds": job.duration_seconds},
                )

                return f"Job ID {job_id} completed successfully"

            except SoftTimeLimitExceeded:
                error_msg = "Pipeline execution exceeded time limit"
                logger.error(error_msg, extra={"job_id": job_id})
                _mark_job_failed(session, job, error_msg, is_infra_error=True)
                raise  # Don't retry on timeout

            except Exception as e:
                logger.error(
                    "Pipeline execution failed",
                    extra={
                        "job_id": job_id,
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "retries": self.request.retries,
                    },
                    exc_info=True,
                )

                # Check if we should retry
                should_retry = _should_retry_job(job, e, self.request.retries)

                if should_retry and self.request.retries < self.max_retries:
                    _mark_job_retrying(session, job, str(e))
                    # Retry with exponential backoff
                    raise self.retry(
                        exc=e, countdown=_calculate_retry_delay(self.request.retries)
                    )
                else:
                    _mark_job_failed(session, job, str(e), is_infra_error=False)
                    raise

    except Retry:
        # Let retry exception propagate
        raise

    except Exception as e:
        logger.error(
            "Unexpected error in execute_pipeline_task",
            extra={"job_id": job_id, "error": str(e)},
            exc_info=True,
        )
        raise


@celery_app.task(
    name="app.worker.tasks.scheduler_heartbeat",
    soft_time_limit=300,  # 5 minutes
    time_limit=360,  # 6 minutes hard limit
)
def scheduler_heartbeat() -> str:
    """
    Check and trigger scheduled pipelines.
    This task should be called periodically (e.g., every minute via Celery Beat).

    Returns:
        Success message with statistics
    """
    logger.info("Scheduler heartbeat started")

    try:
        from app.engine.scheduler import Scheduler

        with session_scope() as session:
            scheduler = Scheduler(session)

            # Track metrics
            start_time = datetime.now(timezone.utc)
            scheduler.check_schedules()
            duration = (datetime.now(timezone.utc) - start_time).total_seconds()

            logger.info(
                "Scheduler heartbeat completed", extra={"duration_seconds": duration}
            )

            return f"Scheduler heartbeat completed in {duration:.2f}s"

    except SoftTimeLimitExceeded:
        logger.error("Scheduler heartbeat exceeded time limit")
        raise

    except Exception as e:
        logger.error(
            "Scheduler heartbeat failed", extra={"error": str(e)}, exc_info=True
        )
        raise


@celery_app.task(
    name="app.worker.tasks.cleanup_old_jobs",
    soft_time_limit=600,  # 10 minutes
)
def cleanup_old_jobs(days_to_keep: int = 30) -> str:
    """
    Clean up old completed jobs and their associated data.

    Args:
        days_to_keep: Number of days of job history to retain

    Returns:
        Success message with cleanup statistics
    """
    logger.info(f"Cleanup task started (keeping {days_to_keep} days)")

    try:
        from sqlalchemy import and_
        from app.models.execution import PipelineRun, StepRun

        with session_scope() as session:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)

            # Count jobs to delete
            old_jobs = (
                session.query(Job)
                .filter(
                    and_(
                        Job.completed_at < cutoff_date,
                        Job.status.in_([JobStatus.SUCCESS, JobStatus.FAILED]),
                    )
                )
                .all()
            )

            job_count = len(old_jobs)

            # Delete associated pipeline runs and step runs (cascade should handle this)
            for job in old_jobs:
                session.delete(job)

            session.commit()

            logger.info(
                f"Cleanup completed: deleted {job_count} old jobs",
                extra={
                    "deleted_count": job_count,
                    "cutoff_date": cutoff_date.isoformat(),
                },
            )

            return f"Cleaned up {job_count} jobs older than {days_to_keep} days"

    except Exception as e:
        logger.error(f"Cleanup task failed: {e}", exc_info=True)
        raise


# Helper functions


def _mark_job_failed(
    session, job: Job, error_message: str, is_infra_error: bool = False
) -> None:
    """Mark a job as failed and log the error."""
    job.status = JobStatus.FAILED
    job.completed_at = datetime.now(timezone.utc)

    if job.started_at:
        job.duration_seconds = (job.completed_at - job.started_at).total_seconds()

    if is_infra_error:
        job.infra_error = error_message
    else:
        job.error_message = error_message

    session.commit()

    DBLogger.log_job(
        session,
        job.id,
        "ERROR",
        f"Job failed: {error_message}",
        source="worker",
        tenant_id=job.tenant_id,
    )


def _mark_job_retrying(session, job: Job, error_message: str) -> None:
    """Mark a job as retrying."""
    job.status = JobStatus.PENDING  # Back to pending for retry
    job.error_message = f"Retry after error: {error_message}"
    session.commit()

    DBLogger.log_job(
        session,
        job.id,
        "WARNING",
        f"Job will be retried: {error_message}",
        source="worker",
        tenant_id=job.tenant_id,
    )


def _should_retry_job(job: Job, error: Exception, retry_count: int) -> bool:
    """
    Determine if a job should be retried based on error type and job configuration.
    """
    # Don't retry certain error types
    non_retryable_errors = (
        ConfigurationError,  # Configuration issues won't be fixed by retrying
        ValueError,  # Invalid data won't be fixed by retrying
    )

    if isinstance(error, non_retryable_errors):
        logger.info(
            f"Not retrying job {job.id} due to non-retryable error type: {type(error).__name__}"
        )
        return False

    # Check job-specific retry strategy if available
    if hasattr(job, "retry_strategy") and job.retry_strategy:
        max_retries = job.retry_strategy.get("max_retries", 3)
        if retry_count >= max_retries:
            return False

    return True


def _calculate_retry_delay(retry_count: int) -> int:
    """
    Calculate retry delay with exponential backoff.
    Returns delay in seconds.
    """
    # Exponential backoff: 60s, 120s, 240s, etc.
    base_delay = 60
    max_delay = 600  # 10 minutes max

    delay = min(base_delay * (2**retry_count), max_delay)
    return delay


# Import at bottom to avoid circular imports
from datetime import timedelta
from app.core.errors import ConfigurationError
