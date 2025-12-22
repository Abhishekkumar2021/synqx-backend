from sqlalchemy.orm import Session
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import json
import redis

from app.models.monitoring import JobLog, StepLog
from app.core.logging import get_logger
from app.core.config import settings

logger = get_logger(__name__)

# Initialize Redis client for publishing events
redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)

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
            timestamp = datetime.now(timezone.utc)
            log_id = None
            
            # Use a savepoint to ensure log failures don't abort the main transaction
            with session.begin_nested():
                log_entry = JobLog(
                    job_id=job_id,
                    level=level.upper(),
                    message=message,
                    metadata_payload=metadata,
                    timestamp=timestamp,
                    source=source,
                )
                session.add(log_entry)
                session.flush() # Flush to assign ID
                log_id = log_entry.id
            
            # Publish to Redis channel
            payload = {
                "type": "job_log",
                "id": log_id,
                "job_id": job_id,
                "level": level.upper(),
                "message": message,
                "timestamp": timestamp.isoformat(),
                "source": source
            }
            redis_client.publish(f"job:{job_id}", json.dumps(payload))
            
        except Exception as e:
            # Fallback to standard logger if DB write fails, to ensure we don't lose the error
            logger.error(f"Failed to write JobLog (Job {job_id}): {e}")

    @staticmethod
    def log_step(session: Session, step_run_id: int, level: str, message: str, metadata: Optional[Dict[str, Any]] = None, source: str = "runner"):
        """
        Writes a log entry to the step_logs table.
        """
        try:
            timestamp = datetime.now(timezone.utc)
            log_id = None

            with session.begin_nested():
                log_entry = StepLog(
                    step_run_id=step_run_id,
                    level=level.upper(),
                    message=message,
                    metadata_payload=metadata,
                    timestamp=timestamp,
                    source=source,
                )
                session.add(log_entry)
                session.flush()
                log_id = log_entry.id
            
            # Publish to Redis channel
            payload = {
                "type": "step_log",
                "id": log_id,
                "step_run_id": step_run_id,
                "level": level.upper(),
                "message": message,
                "timestamp": timestamp.isoformat(),
                "source": source
            }
            redis_client.publish(f"step:{step_run_id}", json.dumps(payload))
            
        except Exception as e:
            logger.error(f"Failed to write StepLog (StepRun {step_run_id}): {e}")