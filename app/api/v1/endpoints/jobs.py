from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session

from app.schemas.job import (
    JobRead,
    JobListResponse,
    JobCancelRequest,
    JobRetryRequest,
    JobLogRead,
    PipelineRunRead,
    PipelineRunDetailRead,
    PipelineRunListResponse,
    StepRunRead,
    StepLogRead,
)
from app.services.job_service import JobService, PipelineRunService
from app.api.deps import get_db
from app.core.errors import AppError
from app.core.logging import get_logger
from app.models.enums import JobStatus, PipelineRunStatus

router = APIRouter()
logger = get_logger(__name__)


@router.get(
    "/jobs",
    response_model=JobListResponse,
    summary="List Jobs",
    description="List all jobs with optional filtering"
)
def list_jobs(
    pipeline_id: Optional[int] = Query(None, description="Filter by pipeline"),
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        service = JobService(db)
        jobs, total = service.list_jobs(
            pipeline_id=pipeline_id,
            status=status,
            limit=limit,
            offset=offset
        )
        
        return JobListResponse(
            jobs=[JobRead.model_validate(j) for j in jobs],
            total=total,
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        logger.error(f"Error listing jobs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to list jobs"}
        )


@router.get(
    "/jobs/{job_id}",
    response_model=JobRead,
    summary="Get Job",
    description="Get detailed information about a specific job"
)
def get_job(
    job_id: int,
    db: Session = Depends(get_db),
):
    service = JobService(db)
    job = service.get_job(job_id)
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not found", "message": f"Job {job_id} not found"}
        )
    
    return JobRead.model_validate(job)


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobRead,
    summary="Cancel Job",
    description="Cancel a running or pending job"
)
def cancel_job(
    job_id: int,
    cancel_request: JobCancelRequest = Body(default=JobCancelRequest()),
    db: Session = Depends(get_db),
):
    try:
        service = JobService(db)
        job = service.cancel_job(job_id, reason=cancel_request.reason)
        return JobRead.model_validate(job)
        
    except AppError as e:
        logger.error(f"Error cancelling job {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)}
        )
    except Exception as e:
        logger.error(f"Unexpected error cancelling job {job_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to cancel job"}
        )


@router.post(
    "/jobs/{job_id}/retry",
    response_model=JobRead,
    summary="Retry Job",
    description="Retry a failed job"
)
def retry_job(
    job_id: int,
    retry_request: JobRetryRequest = Body(default=JobRetryRequest()),
    db: Session = Depends(get_db),
):
    try:
        service = JobService(db)
        new_job = service.retry_job(job_id, force=retry_request.force)
        return JobRead.model_validate(new_job)
        
    except AppError as e:
        logger.error(f"Error retrying job {job_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)}
        )
    except Exception as e:
        logger.error(f"Unexpected error retrying job {job_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to retry job"}
        )


@router.get(
    "/jobs/{job_id}/logs",
    response_model=List[JobLogRead],
    summary="Get Job Logs",
    description="Get logs for a specific job"
)
def get_job_logs(
    job_id: int,
    level: Optional[str] = Query(None, description="Filter by log level"),
    db: Session = Depends(get_db),
):
    try:
        service = JobService(db)
        job = service.get_job(job_id)
        
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Not found", "message": f"Job {job_id} not found"}
            )
        
        logs = service.get_job_logs(job_id, level=level)
        return [JobLogRead.model_validate(log) for log in logs]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting job logs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to get job logs"}
        )


@router.get(
    "/runs",
    response_model=PipelineRunListResponse,
    summary="List Pipeline Runs",
    description="List all pipeline runs with optional filtering"
)
def list_runs(
    pipeline_id: Optional[int] = Query(None, description="Filter by pipeline"),
    status: Optional[PipelineRunStatus] = Query(None, description="Filter by status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        service = PipelineRunService(db)
        runs, total = service.list_runs(
            pipeline_id=pipeline_id,
            status=status,
            limit=limit,
            offset=offset
        )
        
        return PipelineRunListResponse(
            runs=[PipelineRunRead.model_validate(r) for r in runs],
            total=total,
            limit=limit,
            offset=offset
        )
        
    except Exception as e:
        logger.error(f"Error listing runs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to list runs"}
        )


@router.get(
    "/runs/{run_id}",
    response_model=PipelineRunDetailRead,
    summary="Get Pipeline Run",
    description="Get detailed information about a specific pipeline run including step runs"
)
def get_run(
    run_id: int,
    db: Session = Depends(get_db),
):
    service = PipelineRunService(db)
    run = service.get_run(run_id)
    
    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not found", "message": f"Pipeline run {run_id} not found"}
        )
    
    response = PipelineRunDetailRead.model_validate(run)
    
    step_runs = service.get_run_steps(run_id)
    response.step_runs = [StepRunRead.model_validate(s) for s in step_runs]
    
    return response


@router.get(
    "/runs/{run_id}/steps",
    response_model=List[StepRunRead],
    summary="Get Step Runs",
    description="Get all step runs for a pipeline run"
)
def get_run_steps(
    run_id: int,
    db: Session = Depends(get_db),
):
    try:
        service = PipelineRunService(db)
        steps = service.get_run_steps(run_id)
        return [StepRunRead.model_validate(s) for s in steps]
        
    except AppError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not found", "message": str(e)}
        )
    except Exception as e:
        logger.error(f"Error getting step runs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to get step runs"}
        )


@router.get(
    "/runs/{run_id}/steps/{step_id}/logs",
    response_model=List[StepLogRead],
    summary="Get Step Logs",
    description="Get logs for a specific step run"
)
def get_step_logs(
    run_id: int,
    step_id: int,
    level: Optional[str] = Query(None, description="Filter by log level"),
    db: Session = Depends(get_db),
):
    try:
        service = PipelineRunService(db)
        logs = service.get_step_logs(step_id, level=level)
        return [StepLogRead.model_validate(log) for log in logs]
        
    except Exception as e:
        logger.error(f"Error getting step logs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to get step logs"}
        )


@router.get(
    "/pipelines/{pipeline_id}/metrics",
    summary="Get Pipeline Metrics",
    description="Get aggregated metrics for a pipeline's runs"
)
def get_pipeline_metrics(
    pipeline_id: int,
    db: Session = Depends(get_db),
):
    try:
        service = PipelineRunService(db)
        metrics = service.get_run_metrics(pipeline_id)
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting pipeline metrics: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "Internal server error", "message": "Failed to get pipeline metrics"}
        )