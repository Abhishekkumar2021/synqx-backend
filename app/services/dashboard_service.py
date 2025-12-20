from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, desc

from app.models.pipelines import Pipeline
from app.models.execution import Job
from app.models.connections import Connection
from app.models.enums import PipelineStatus, JobStatus
from app.schemas.dashboard import DashboardStats, ThroughputDataPoint, PipelineDistribution, RecentActivity
from app.core.logging import get_logger

logger = get_logger(__name__)

class DashboardService:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_stats(self, user_id: int) -> DashboardStats:
        try:
            now = datetime.now(timezone.utc)
            one_day_ago = now - timedelta(days=1)

            # 1. Global Metrics
            total_pipelines = self.db.query(func.count(Pipeline.id)).filter(
                and_(Pipeline.user_id == user_id, Pipeline.deleted_at.is_(None))
            ).scalar() or 0

            active_pipelines = self.db.query(func.count(Pipeline.id)).filter(
                and_(Pipeline.user_id == user_id, Pipeline.status == PipelineStatus.ACTIVE, Pipeline.deleted_at.is_(None))
            ).scalar() or 0

            total_connections = self.db.query(func.count(Connection.id)).filter(
                and_(Connection.user_id == user_id, Connection.deleted_at.is_(None))
            ).scalar() or 0

            # Jobs in last 24h
            jobs_24h_query = self.db.query(
                func.count(Job.id).label("total"),
                func.sum(case((Job.status == JobStatus.SUCCESS, 1), else_=0)).label("success"),
                func.avg(Job.execution_time_ms).label("avg_duration")
            ).join(Pipeline).filter(
                and_(
                    Pipeline.user_id == user_id,
                    Job.created_at >= one_day_ago
                )
            )
            
            jobs_stats = jobs_24h_query.first()
            total_jobs_24h = jobs_stats.total or 0
            success_jobs_24h = jobs_stats.success or 0
            success_rate_24h = (success_jobs_24h / total_jobs_24h * 100) if total_jobs_24h > 0 else 0.0
            avg_duration_ms = float(jobs_stats.avg_duration or 0)
            avg_duration_24h = avg_duration_ms / 1000.0

            # 2. Pipeline Distribution
            dist_query = self.db.query(
                Pipeline.status, func.count(Pipeline.id)
            ).filter(
                and_(Pipeline.user_id == user_id, Pipeline.deleted_at.is_(None))
            ).group_by(Pipeline.status).all()
            
            distribution = [
                PipelineDistribution(status=status.value, count=count) 
                for status, count in dist_query
            ]

            # 3. Throughput (Last 24h, grouped by hour)
            # Detect dialect to choose truncation method
            if self.db.bind.dialect.name == 'sqlite':
                hour_col = func.strftime('%Y-%m-%d %H:00:00', Job.created_at)
            else:
                hour_col = func.date_trunc('hour', Job.created_at)
            
            hourly_stats = self.db.query(
                hour_col.label('hour'),
                func.sum(case((Job.status == JobStatus.SUCCESS, 1), else_=0)).label("success"),
                func.sum(case((Job.status == JobStatus.FAILED, 1), else_=0)).label("failure")
            ).join(Pipeline).filter(
                and_(
                    Pipeline.user_id == user_id,
                    Job.created_at >= one_day_ago
                )
            ).group_by(hour_col).order_by(hour_col).all()

            throughput = []
            for row in hourly_stats:
                # Handle SQLite string return vs Postgres datetime return
                ts = row.hour
                if isinstance(ts, str):
                    try:
                        ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except ValueError:
                        pass # Keep as is or handle error
                
                throughput.append(ThroughputDataPoint(
                    timestamp=ts,
                    success_count=row.success or 0,
                    failure_count=row.failure or 0
                ))

            # 4. Recent Activity
            recent_jobs = self.db.query(Job).join(Pipeline).filter(
                Pipeline.user_id == user_id
            ).order_by(desc(Job.created_at)).limit(10).all()

            activity = []
            for job in recent_jobs:
                duration = None
                if job.execution_time_ms:
                    duration = job.execution_time_ms / 1000.0
                
                activity.append(RecentActivity(
                    id=job.id,
                    pipeline_id=job.pipeline_id,
                    pipeline_name=job.pipeline.name,
                    status=job.status.value,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    duration_seconds=duration,
                    user_avatar=None 
                ))

            return DashboardStats(
                total_pipelines=total_pipelines,
                active_pipelines=active_pipelines,
                total_jobs_24h=total_jobs_24h,
                success_rate_24h=round(success_rate_24h, 1),
                avg_duration_24h=round(avg_duration_24h, 2),
                total_connections=total_connections,
                throughput=throughput,
                pipeline_distribution=distribution,
                recent_activity=activity
            )
        except Exception as e:
            logger.error("Error generating dashboard stats", error=str(e), exc_info=True)
            raise e
