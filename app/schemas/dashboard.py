from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict

class ThroughputDataPoint(BaseModel):
    timestamp: datetime
    success_count: int
    failure_count: int
    rows_processed: int = 0
    bytes_processed: int = 0

    model_config = ConfigDict(from_attributes=True)

class PipelineDistribution(BaseModel):
    status: str
    count: int

class RecentActivity(BaseModel):
    id: int
    pipeline_id: int
    pipeline_name: str
    status: str
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    user_avatar: Optional[str] = None # Placeholder for frontend compatibility

class DashboardStats(BaseModel):
    total_pipelines: int
    active_pipelines: int
    total_jobs_24h: int
    success_rate_24h: float
    avg_duration_24h: float
    total_connections: int
    total_rows_24h: int = 0
    total_bytes_24h: int = 0
    
    throughput: List[ThroughputDataPoint]
    pipeline_distribution: List[PipelineDistribution]
    recent_activity: List[RecentActivity]

    model_config = ConfigDict(from_attributes=True)
