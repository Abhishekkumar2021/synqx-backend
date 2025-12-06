from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import settings

# Create the SQLAlchemy engine
# pool_pre_ping=True ensures the connection is valid before using it
engine = create_engine(
    settings.DATABASE_URL, 
    pool_pre_ping=True,
    echo=False
)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """
    Dependency for FastAPI path operations.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


from contextlib import contextmanager

@contextmanager
def session_scope():
    """
    Context manager for database sessions, useful for Celery tasks
    and other background operations.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()