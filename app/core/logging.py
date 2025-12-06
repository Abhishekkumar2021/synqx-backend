import logging
import sys
import structlog
from app.core.config import settings

def setup_logging():
    """
    Configures structlog and standard logging.
    """
    # Define processors that are common to both structlog and stdlib logging
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # structlog configuration
    structlog.configure(
        processors=shared_processors + [
            # Prepare event dict for stdlib logging
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Formatting for the standard library logger
    formatter = structlog.stdlib.ProcessorFormatter(
        # These run on log entries that originate from standard logging (not structlog)
        foreign_pre_chain=shared_processors,
        # These run on ALL log entries (both structlog and stdlib) to do final rendering
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer() if settings.JSON_LOGS else structlog.dev.ConsoleRenderer(),
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    # Clear existing handlers to prevent duplicate logs
    root_logger.handlers = []
    root_logger.addHandler(handler)
    root_logger.setLevel(settings.LOG_LEVEL.upper())

    # Quiet down some noisy libraries
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)

    if settings.ENVIRONMENT == "development":
        logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
    
    # Recreate the logger for this module to pick up changes (if needed) or just return a structlog logger
    
def get_logger(name: str):
    return structlog.get_logger(name)
