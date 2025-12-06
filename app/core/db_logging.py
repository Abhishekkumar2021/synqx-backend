from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from app.models.monitoring import JobLog, StepLog
from app.core.logging import get_logger

logger = get_logger(__name__)

class DBLogger:
    """
    Helper class to write logs to the database for Jobs and Steps.
    Uses the existing SQLAlchemy session.
    """

    @staticmethod
    def log_job(session: Session, job_id: int, level: str, message: str, metadata: Optional[Dict[str, Any]] = None, source: str = "system"):
        """
        Writes a log entry to the job_logs table.
        """
        try:
            log_entry = JobLog(
                job_id=job_id,
                level=level.upper(),
                message=message,
                metadata_payload=metadata,
                timestamp=datetime.now(timezone.utc),
                source=source,
            )
            session.add(log_entry)
            session.flush() # Flush to assign ID and ensure it's pending commit
        except Exception as e:
            # Fallback to standard logger if DB write fails, to ensure we don't lose the error
            logger.error(f"Failed to write JobLog (Job {job_id}): {e}")

    @staticmethod
    def log_step(session: Session, step_run_id: int, level: str, message: str, metadata: Optional[Dict[str, Any]] = None, source: str = "runner"):
        """
        Writes a log entry to the step_logs table.
        """
        try:
            log_entry = StepLog(
                step_run_id=step_run_id,
                level=level.upper(),
                message=message,
                metadata_payload=metadata,
                timestamp=datetime.now(timezone.utc),
                source=source,
            )
            session.add(log_entry)
            session.flush()
        except Exception as e:
            logger.error(f"Failed to write StepLog (StepRun {step_run_id}): {e}")