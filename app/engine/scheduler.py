import uuid
from datetime import datetime, timezone
from typing import Optional
from croniter import croniter
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.models.pipelines import Pipeline
from app.models.monitoring import SchedulerEvent
from app.models.execution import Job
from app.models.enums import JobStatus
from app.worker.tasks import execute_pipeline_task
from app.core.logging import get_logger

logger = get_logger(__name__)

class Scheduler:
    """
    Responsible for checking pipeline schedules and triggering execution.
    """
    def __init__(self, db_session: Session):
        self.db_session = db_session

    def check_schedules(self) -> None:
        """
        Iterates through all enabled scheduled pipelines and triggers them if due.
        """
        logger.info("Scheduler: Checking schedules...")
        
        pipelines = self.db_session.query(Pipeline).filter(
            Pipeline.schedule_enabled == True,
            Pipeline.schedule_cron.isnot(None),
            Pipeline.deleted_at.is_(None)
        ).all()

        for pipeline in pipelines:
            try:
                self._process_pipeline_schedule(pipeline)
            except Exception as e:
                logger.error(f"Error processing schedule for pipeline {pipeline.id}: {e}", exc_info=True)

    def _process_pipeline_schedule(self, pipeline: Pipeline) -> None:
        """
        Checks if a single pipeline is due for execution.
        """
        if not pipeline.schedule_cron:
            return

        last_run_time = self._get_last_scheduled_run_time(pipeline.id)
        
        # Use current time in UTC
        now = datetime.now(timezone.utc)
        
        # If no last run, use pipeline creation time or a fallback
        if not last_run_time:
            # If created_at is naive, assume UTC or make it aware
            last_run_time = pipeline.created_at or now
            if last_run_time.tzinfo is None:
                 last_run_time = last_run_time.replace(tzinfo=timezone.utc)

        try:
            # Create croniter instance based on last run time
            cron = croniter(pipeline.schedule_cron, last_run_time)
            next_run_time = cron.get_next(datetime)
            
            # If next_run_time is in the past (<= now), it's due
            if next_run_time <= now:
                logger.info(f"Pipeline {pipeline.id} is due. Next run was: {next_run_time}, Now: {now}")
                self._trigger_scheduled_run(pipeline, next_run_time)
            else:
                logger.debug(f"Pipeline {pipeline.id} not due. Next run: {next_run_time}")

        except Exception as e:
            logger.error(f"Failed to calculate schedule for pipeline {pipeline.id}: {e}")

    def _get_last_scheduled_run_time(self, pipeline_id: int) -> Optional[datetime]:
        """
        Retrieves the timestamp of the last *scheduled* run.
        """
        last_event = self.db_session.query(SchedulerEvent).filter(
            SchedulerEvent.pipeline_id == pipeline_id,
            SchedulerEvent.event_type == "TRIGGER"
        ).order_by(desc(SchedulerEvent.timestamp)).first()

        if last_event:
            return last_event.timestamp
        return None

    def _trigger_scheduled_run(self, pipeline: Pipeline, scheduled_time: datetime) -> None:
        """
        Triggers the pipeline run and records the scheduler event.
        """
        # 1. Create Scheduler Event
        event = SchedulerEvent(
            pipeline_id=pipeline.id,
            event_type="TRIGGER",
            timestamp=scheduled_time, # Record the *scheduled* time, or actual time? 
            # Usually actual time of trigger is better for 'timestamp', 
            # but using scheduled_time helps avoid drift if we use it for next calc.
            # However, croniter uses start_time to calc next. 
            # Let's use now() for the event timestamp to record when we actually triggered it.
            # But for the next calculation, we rely on the DB event. 
            # To avoid double firing if we run every minute and the task takes 2 seconds:
            # We record 'now'. Next time, 'last_run' is 'now'.
            cron_expression=pipeline.schedule_cron,
            reason="Scheduled execution"
        )
        # Use actual current time for the event log
        event.timestamp = datetime.now(timezone.utc)
        
        self.db_session.add(event)
        
        # 2. Create Job
        if not pipeline.published_version_id:
             logger.warning(f"Pipeline {pipeline.id} has no published version. Skipping schedule.")
             return

        job = Job(
            pipeline_id=pipeline.id,
            pipeline_version_id=pipeline.published_version_id,
            correlation_id=str(uuid.uuid4()),
            status=JobStatus.PENDING,
            retry_strategy=pipeline.retry_strategy if hasattr(pipeline, 'retry_strategy') else None
        )
        self.db_session.add(job)
        self.db_session.flush()

        # 3. Enqueue Celery Task
        try:
            task = execute_pipeline_task.delay(job.id)
            job.celery_task_id = task.id
            self.db_session.commit()
            logger.info(f"Triggered scheduled run for pipeline {pipeline.id}. Job ID: {job.id}, Task ID: {task.id}")
        except Exception as e:
            logger.error(f"Failed to enqueue task for pipeline {pipeline.id}: {e}")
            self.db_session.rollback()