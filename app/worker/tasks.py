import setuptools  # Patch for Python 3.12+ distutils removal
from datetime import datetime, timezone
from typing import Optional
from celery.exceptions import SoftTimeLimitExceeded, Retry
from app.core.celery_app import celery_app
from app.core.logging import get_logger
from app.db.session import session_scope
from app.models.execution import Job, PipelineRun
from app.models.enums import JobStatus, PipelineRunStatus
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
            if job.status == JobStatus.SUCCESS:
                logger.warning(
                    f"Job {job_id} already COMPLETED",
                    extra={"job_id": job_id, "status": job.status.value},
                )
                return f"Job {job_id} already {job.status.value}"

            if job.status == JobStatus.RUNNING:
                if self.request.retries > 0:
                    logger.warning(
                        f"Job {job_id} found in RUNNING state during retry. Assuming crash recovery.",
                        extra={"job_id": job_id, "retries": self.request.retries},
                    )
                    # Reset status to allow execution to proceed
                    # We don't return here; we let it fall through to "Mark job as running" update below
                else:
                    logger.warning(
                        f"Job {job_id} already RUNNING (no retry)",
                        extra={"job_id": job_id},
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
            )

            # Trigger Job Started Alert
            try:
                from app.services.alert_service import AlertService
                from app.models.enums import AlertType, AlertLevel
                # Use a separate session for alerts to avoid aborting the main transaction on error
                with session_scope() as alert_session:
                    AlertService.trigger_alerts(
                        alert_session,
                        alert_type=AlertType.JOB_STARTED,
                        pipeline_id=job.pipeline_id,
                        job_id=job.id,
                        message=f"Pipeline execution started (attempt {self.request.retries + 1})",
                        level=AlertLevel.INFO
                    )
            except Exception as alert_err:
                logger.error(f"Failed to create start alerts: {alert_err}")

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

                # If runner.run completes without exception, all pipeline_run and step_run changes are flushed
                # and pipeline_run.status is set to COMPLETED.
                # Mark job as successful
                job.status = JobStatus.SUCCESS
                job.completed_at = datetime.now(timezone.utc)

                # Get the associated PipelineRun to retrieve its duration
                pipeline_run = session.query(PipelineRun).filter(PipelineRun.job_id == job.id).first()
                if pipeline_run and pipeline_run.duration_seconds is not None:
                    job.execution_time_ms = int(pipeline_run.duration_seconds * 1000)
                else:
                    # Fallback if pipeline_run is not found or duration not set
                    if job.started_at:
                        duration_seconds = (job.completed_at - job.started_at).total_seconds()
                        job.execution_time_ms = int(duration_seconds * 1000)
                    else:
                        job.execution_time_ms = 0 # Default if started_at is also missing

                session.commit() # Final commit of all changes in the session

                # Trigger Success Alerts
                try:
                    from app.services.alert_service import AlertService
                    from app.models.enums import AlertType, AlertLevel
                    with session_scope() as alert_session:
                        AlertService.trigger_alerts(
                            alert_session,
                            alert_type=AlertType.JOB_SUCCESS,
                            pipeline_id=job.pipeline_id,
                            job_id=job.id,
                            message=f"Pipeline execution completed successfully",
                            level=AlertLevel.SUCCESS
                        )
                except Exception as alert_err:
                    logger.error(f"Failed to create success alerts: {alert_err}")

                DBLogger.log_job(
                    session,
                    job.id,
                    "INFO",
                    "Job completed successfully",
                    source="worker",
                )

                logger.info(
                    "Pipeline execution completed",
                    extra={
                        "job_id": job_id,
                        "duration_seconds": (job.execution_time_ms / 1000.0 if job.execution_time_ms else 0.0),
                    },
                )

                return f"Job ID {job_id} completed successfully"

            except SoftTimeLimitExceeded:
                error_msg = "Pipeline execution exceeded time limit"
                logger.error(error_msg, extra={"job_id": job_id})
                # Set pipeline_run status to failed here as runner.run might not have
                # caught this specific exception type or the session might be invalid.
                # Find the pipeline_run in the session and update it.
                pipeline_run_in_session = (
                    session.query(PipelineRun)
                    .filter(PipelineRun.job_id == job_id)
                    .first()
                )
                if pipeline_run_in_session:
                    pipeline_run_in_session.status = PipelineRunStatus.FAILED
                    pipeline_run_in_session.completed_at = datetime.now(timezone.utc)
                    pipeline_run_in_session.error_message = error_msg
                    session.add(pipeline_run_in_session)
                
                _mark_job_failed(session, job, error_msg, is_infra_error=True)
                raise  # Don't retry on timeout

            except Exception as e:
                # This catches exceptions from runner.run
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

                # Check if the job actually succeeded (e.g. error happened during post-processing logs/alerts)
                # We open a new session to check the committed state
                try:
                    with session_scope() as check_session:
                        current_job = check_session.query(Job).filter(Job.id == job_id).first()
                        if current_job and current_job.status == JobStatus.SUCCESS:
                            logger.error(f"Job {job_id} succeeded but post-processing failed: {e}")
                            return f"Job {job_id} completed successfully (with post-processing errors)"
                except Exception as check_err:
                    logger.error(f"Failed to verify job status after error: {check_err}")

                # At this point, runner.run should have already marked pipeline_run as FAILED and flushed.
                # We just need to handle the job status and commit/retry.
                should_retry = _should_retry_job(job, e, self.request.retries)

                if should_retry and self.request.retries < self.max_retries:
                    # _mark_job_retrying commits the session
                    _mark_job_retrying(session, job, str(e))
                    raise self.retry(
                        exc=e, countdown=_calculate_retry_delay(self.request.retries)
                    )
                else:
                    # _mark_job_failed commits the session
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
        # Attempt to mark the job as failed if it's not already
        with session_scope() as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if job and job.status not in [JobStatus.SUCCESS, JobStatus.FAILED]:
                _mark_job_failed(session, job, f"Unexpected worker error: {e}", is_infra_error=True)
            # Find the pipeline_run in the session and update it.
            pipeline_run_in_session = (
                session.query(PipelineRun)
                .filter(PipelineRun.job_id == job_id)
                .first()
            )
            if pipeline_run_in_session and pipeline_run_in_session.status not in [PipelineRunStatus.COMPLETED, PipelineRunStatus.FAILED]:
                pipeline_run_in_session.status = PipelineRunStatus.FAILED
                pipeline_run_in_session.completed_at = datetime.now(timezone.utc)
                pipeline_run_in_session.error_message = f"Unexpected worker error: {e}"
                session.add(pipeline_run_in_session)
                session.commit()
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
    from app.models.monitoring import Alert, AlertConfig
    from app.models.pipelines import Pipeline

    job.status = JobStatus.FAILED
    job.completed_at = datetime.now(timezone.utc)

    # Get the associated PipelineRun to set its duration
    pipeline_run = session.query(PipelineRun).filter(PipelineRun.job_id == job.id).first()
    if pipeline_run:
        if pipeline_run.started_at:
            pipeline_run.duration_seconds = (
                pipeline_run.completed_at - pipeline_run.started_at
            ).total_seconds()
            job.execution_time_ms = int(pipeline_run.duration_seconds * 1000)
        session.add(pipeline_run) # Persist changes to pipeline_run
    else:
        if job.started_at:
            duration_seconds = (job.completed_at - job.started_at).total_seconds()
            job.execution_time_ms = int(duration_seconds * 1000)

    if is_infra_error:
        job.infra_error = error_message
    else:
        job.infra_error = f"Execution Error: {error_message}"

    session.commit()

    # Trigger Alerts based on Config
    try:
        from app.services.alert_service import AlertService
        from app.models.enums import AlertType, AlertLevel
        
        with session_scope() as alert_session:
            AlertService.trigger_alerts(
                alert_session,
                alert_type=AlertType.JOB_FAILURE,
                pipeline_id=job.pipeline_id,
                job_id=job.id,
                message=error_message,
                level=AlertLevel.ERROR
            )
    except Exception as alert_err:
        logger.error(f"Failed to create alerts: {alert_err}")

    DBLogger.log_job(
        session, job.id, "ERROR", f"Job failed: {error_message}", source="worker"
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
    # max_retries is a direct attribute on Job model
    if hasattr(job, "max_retries"):
        if retry_count >= job.max_retries:
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
