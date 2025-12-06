import time
from celery import shared_task
from sqlalchemy import text
from app.core.celery_app import celery_app
from app.core.logging import get_logger
from app.db.session import session_scope

logger = get_logger(__name__)

@celery_app.task(name="app.worker.tasks.test_celery")
def test_celery(word: str) -> str:
    logger.info("test_celery_task_started", word=word)
    return f"test task return {word}"

@celery_app.task(name="app.worker.tasks.db_check_task")
def db_check_task() -> str:
    """
    Example task demonstrating DB session usage.
    """
    logger.info("db_check_task_started")
    
    try:
        with session_scope() as session:
            # Simple query to verify DB connection
            result = session.execute(text("SELECT 1")).scalar()
            logger.info("db_check_task_success", result=result)
            return "DB Connection OK"
            
    except Exception as e:
        logger.error("db_check_task_failed", error=str(e))
        raise e