from typing import Optional, Dict
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.schemas.pipeline import PipelineCreate, PipelineRead, PipelineVersionRead
from app.services.pipeline_service import PipelineService
from app.api.deps import get_db
from app.core.errors import AppError
from app.core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)

# Placeholder for tenant_id. In a real app, this would come from authentication/session.
CURRENT_TENANT_ID = 1

@router.post("/", response_model=PipelineRead, status_code=status.HTTP_201_CREATED)
def create_pipeline(
    pipeline_create: PipelineCreate,
    db: Session = Depends(get_db)
):
    """
    Creates a new pipeline with its initial version.
    """
    try:
        service = PipelineService(db)
        pipeline = service.create_pipeline(pipeline_create, tenant_id=CURRENT_TENANT_ID)
        
        # Optionally load the current version details for the response
        if pipeline.published_version_id:
            version_detail = service.get_pipeline_version(pipeline.id, pipeline.published_version_id, CURRENT_TENANT_ID)
            pipeline_read = PipelineRead.model_validate(pipeline)
            pipeline_read.current_version_detail = PipelineVersionRead.model_validate(version_detail) if version_detail else None
        else:
            pipeline_read = PipelineRead.model_validate(pipeline)

        return pipeline_read
    except AppError as e:
        logger.error(f"Error creating pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error creating pipeline: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred.")


@router.get("/{pipeline_id}", response_model=PipelineRead)
def get_pipeline(
    pipeline_id: int,
    db: Session = Depends(get_db)
):
    """
    Retrieves a pipeline by its ID.
    """
    service = PipelineService(db)
    pipeline = service.get_pipeline(pipeline_id, tenant_id=CURRENT_TENANT_ID)
    if not pipeline:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Pipeline not found")
    
    # Load current version details for the response
    pipeline_read = PipelineRead.model_validate(pipeline)
    if pipeline.published_version_id:
        version_detail = service.get_pipeline_version(pipeline.id, pipeline.published_version_id, CURRENT_TENANT_ID)
        pipeline_read.current_version_detail = PipelineVersionRead.model_validate(version_detail) if version_detail else None

    return pipeline_read

@router.post("/{pipeline_id}/run", response_model=Dict[str, str])
def trigger_pipeline_run(
    pipeline_id: int,
    version_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Triggers an immediate run for a specified pipeline version.
    If version_id is not provided, the currently published version will be used.
    """
    try:
        service = PipelineService(db)
        result = service.trigger_pipeline_run(pipeline_id, CURRENT_TENANT_ID, version_id)
        return result
    except AppError as e:
        logger.error(f"Error triggering pipeline run for {pipeline_id} (version {version_id}): {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error triggering pipeline run for {pipeline_id} (version {version_id}): {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected error occurred during pipeline run.")
