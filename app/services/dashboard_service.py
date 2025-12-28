from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session
from sqlalchemy import func, case, and_, desc, or_

from app.models.pipelines import Pipeline
from app.models.execution import Job, PipelineRun, StepRun
from app.models.monitoring import Alert, AlertConfig
from app.models.connections import Connection
from app.models.enums import PipelineStatus, JobStatus, OperatorRunStatus
from app.schemas.dashboard import (
    DashboardStats, ThroughputDataPoint, PipelineDistribution, RecentActivity,
    SystemHealth, FailingPipeline, SlowestPipeline, DashboardAlert, ConnectorHealth
)
from app.core.logging import get_logger

logger = get_logger(__name__)

class DashboardService:
    def __init__(self, db_session: Session):
        self.db = db_session

    def get_stats(self, user_id: int, time_range: str = "24h") -> DashboardStats:
        try:
            now = datetime.now(timezone.utc)
            start_date = None
            group_interval = 'day'

            if time_range == '24h':
                start_date = now - timedelta(days=1)
                group_interval = 'hour'
            elif time_range == '7d':
                start_date = now - timedelta(days=7)
            elif time_range == '30d':
                start_date = now - timedelta(days=30)
            
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

            # Connector Health Distribution
            connector_health = []
            try:
                health_query = self.db.query(
                    Connection.health_status, func.count(Connection.id)
                ).filter(
                    and_(Connection.user_id == user_id, Connection.deleted_at.is_(None))
                ).group_by(Connection.health_status).all()
                
                connector_health = [
                    ConnectorHealth(status=status, count=count)
                    for status, count in health_query
                ]
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error calculating connector health: {e}")

            # Period Stats
            period_filters = [Pipeline.user_id == user_id]
            if start_date:
                period_filters.append(Job.created_at >= start_date)

            jobs_period_query = self.db.query(
                func.count(Job.id).label("total"),
                func.sum(case((Job.status == JobStatus.SUCCESS, 1), else_=0)).label("success"),
                func.avg(Job.execution_time_ms).label("avg_duration"),
                func.sum(PipelineRun.total_loaded).label("total_rows"),
                func.sum(PipelineRun.bytes_processed).label("total_bytes")
            ).select_from(Job).join(Pipeline, Job.pipeline_id == Pipeline.id).outerjoin(PipelineRun, Job.id == PipelineRun.job_id).filter(
                and_(*period_filters)
            )
            
            jobs_stats = jobs_period_query.first()
            total_jobs = jobs_stats.total or 0
            success_jobs = jobs_stats.success or 0
            success_rate = (success_jobs / total_jobs * 100) if total_jobs > 0 else 0.0
            avg_duration_ms = float(jobs_stats.avg_duration or 0)
            avg_duration = avg_duration_ms / 1000.0
            total_rows = int(jobs_stats.total_rows or 0)
            total_bytes = int(jobs_stats.total_bytes or 0)

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

            # 3. Throughput
            if self.db.bind.dialect.name == 'sqlite':
                if group_interval == 'hour':
                    time_col = func.strftime('%Y-%m-%d %H:00:00', Job.created_at)
                else:
                    time_col = func.strftime('%Y-%m-%d', Job.created_at)
            else:
                time_col = func.date_trunc(group_interval, Job.created_at)
            
            throughput_query = self.db.query(
                time_col.label('time_bucket'),
                func.sum(case((Job.status == JobStatus.SUCCESS, 1), else_=0)).label("success"),
                func.sum(case((Job.status == JobStatus.FAILED, 1), else_=0)).label("failure"),
                func.sum(PipelineRun.total_loaded).label("rows"),
                func.sum(PipelineRun.bytes_processed).label("bytes")
            ).select_from(Job).join(Pipeline, Job.pipeline_id == Pipeline.id).outerjoin(PipelineRun, Job.id == PipelineRun.job_id).filter(
                and_(*period_filters)
            ).group_by(time_col).order_by(time_col)

            throughput_stats = throughput_query.all()
            
            # Zero-filling logic
            throughput_map = {
                (row.time_bucket if isinstance(row.time_bucket, datetime) else 
                 datetime.strptime(row.time_bucket, '%Y-%m-%d %H:00:00' if len(row.time_bucket) > 10 else '%Y-%m-%d').replace(tzinfo=timezone.utc)
                 if isinstance(row.time_bucket, str) else row.time_bucket): row
                for row in throughput_stats
            }

            throughput = []
            current_bucket = start_date if start_date else (now - timedelta(days=30))
            # Ensure current_bucket is timezone aware if start_date came from now (which is utc)
            # Depending on DB, time_bucket from query might be offset-aware or naive.
            # We will generate buckets based on 'now' and match.
            
            # Normalize buckets to simple iteration
            num_buckets = 24 if time_range == '24h' else (7 if time_range == '7d' else 30)
            delta = timedelta(hours=1) if group_interval == 'hour' else timedelta(days=1)
            
            # Reset minutes/seconds for clean buckets
            if group_interval == 'hour':
                current_bucket = current_bucket.replace(minute=0, second=0, microsecond=0)
            else:
                current_bucket = current_bucket.replace(hour=0, minute=0, second=0, microsecond=0)

            for _ in range(num_buckets + 1): # +1 to include current partial bucket
                # Try to find match. Note: timezones might tricky.
                # Simplest is to match by day/hour string or timestamp value equality if possible.
                # Let's use simple matching logic or just append existing data if filling is too complex for mixed TZs.
                # Actually, given the complexity of TZs in python vs DB, a simpler fill approach:
                # We will just iterate the stats we got. If it's sparse, the frontend chart usually handles it okay-ish.
                # But to really fix "empty chart", we need data.
                # Let's stick to the query result for now but ensure we handle the map correctly if we were to fill.
                # Given strict instructions to "zero-fill", I will implement it.
                
                # Check if we have data for this bucket
                # We need to match the key format from throughput_map (datetime)
                # Note: throughput_stats keys might have TZ info. current_bucket has UTC (from now).
                
                # Loose matching: check if any key in map is "close" or has same year/month/day/hour
                match = None
                for key, row in throughput_map.items():
                    # Handle potentially naive vs aware comparison
                    k = key.replace(tzinfo=timezone.utc) if key.tzinfo is None else key.astimezone(timezone.utc)
                    c = current_bucket.replace(tzinfo=timezone.utc) if current_bucket.tzinfo is None else current_bucket.astimezone(timezone.utc)
                    
                    if group_interval == 'hour':
                        if k.year == c.year and k.month == c.month and k.day == c.day and k.hour == c.hour:
                            match = row
                            break
                    else:
                        if k.year == c.year and k.month == c.month and k.day == c.day:
                            match = row
                            break
                
                if match:
                    throughput.append(ThroughputDataPoint(
                        timestamp=current_bucket,
                        success_count=match.success or 0,
                        failure_count=match.failure or 0,
                        rows_processed=int(match.rows or 0),
                        bytes_processed=int(match.bytes or 0)
                    ))
                else:
                     throughput.append(ThroughputDataPoint(
                        timestamp=current_bucket,
                        success_count=0,
                        failure_count=0,
                        rows_processed=0,
                        bytes_processed=0
                    ))
                
                current_bucket += delta

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

            # 5. System Health (Simulated/Recent Aggregates)
            try:
                # Active workers = jobs currently running
                active_workers_count = self.db.query(func.count(Job.id)).join(Pipeline).filter(
                    and_(Pipeline.user_id == user_id, Job.status.in_([JobStatus.RUNNING, JobStatus.PENDING]))
                ).scalar() or 0

                # Average resource usage
                # 1. Try fetching currently running steps
                recent_window = now - timedelta(minutes=15)
                resource_stats = self.db.query(
                    func.avg(StepRun.cpu_percent).label('avg_cpu'),
                    func.avg(StepRun.memory_mb).label('avg_mem')
                ).join(PipelineRun).join(Pipeline).filter(
                    and_(
                        Pipeline.user_id == user_id,
                        StepRun.updated_at >= recent_window,
                        StepRun.status == OperatorRunStatus.RUNNING
                    )
                ).first()

                cpu_val = 0.0
                mem_val = 0.0
                
                # Check if we got valid data (non-None averages)
                if resource_stats and resource_stats.avg_cpu is not None:
                    cpu_val = float(resource_stats.avg_cpu)
                    mem_val = float(resource_stats.avg_mem)
                else:
                    # 2. Fallback: Fetch last 10 successful steps to show "Last Known Capacity"
                    # Fetch raw rows and average in python to avoid GroupingError
                    recent_steps = self.db.query(
                        StepRun.cpu_percent,
                        StepRun.memory_mb
                    ).join(PipelineRun).join(Pipeline).filter(
                        and_(
                            Pipeline.user_id == user_id,
                            StepRun.status == OperatorRunStatus.SUCCESS,
                            StepRun.cpu_percent.isnot(None)
                        )
                    ).order_by(desc(StepRun.updated_at)).limit(10).all()
                    
                    if recent_steps:
                        count = len(recent_steps)
                        cpu_sum = sum(s.cpu_percent or 0 for s in recent_steps)
                        mem_sum = sum(s.memory_mb or 0 for s in recent_steps)
                        cpu_val = cpu_sum / count
                        mem_val = mem_sum / count

                system_health = SystemHealth(
                    cpu_percent=round(cpu_val, 1),
                    memory_usage_mb=round(mem_val, 1),
                    active_workers=active_workers_count
                )
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error calculating system health: {e}")
                system_health = SystemHealth(cpu_percent=0, memory_usage_mb=0, active_workers=0)

            # 6. Top Failing Pipelines
            try:
                failing_query = self.db.query(
                    Pipeline.id,
                    Pipeline.name,
                    func.count(Job.id).label('failures')
                ).join(Job).filter(
                    and_(*period_filters, Job.status == JobStatus.FAILED)
                ).group_by(Pipeline.id, Pipeline.name).order_by(desc('failures')).limit(5)
                
                top_failing = [
                    FailingPipeline(id=r.id, name=r.name, failure_count=r.failures)
                    for r in failing_query.all()
                ]
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error calculating failing pipelines: {e}")
                top_failing = []

            # 7. Slowest Pipelines
            try:
                slowest_query = self.db.query(
                    Pipeline.id,
                    Pipeline.name,
                    func.avg(Job.execution_time_ms).label('avg_duration')
                ).join(Job).filter(
                    and_(*period_filters, Job.status == JobStatus.SUCCESS)
                ).group_by(Pipeline.id, Pipeline.name).order_by(desc('avg_duration')).limit(5)

                slowest_pipelines = [
                    SlowestPipeline(
                        id=r.id, 
                        name=r.name, 
                        avg_duration=round(float(r.avg_duration or 0) / 1000.0, 2)
                    ) for r in slowest_query.all()
                ]
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error calculating slowest pipelines: {e}")
                slowest_pipelines = []

            # 8. Recent Alerts
            try:
                alerts_query = self.db.query(Alert).outerjoin(
                    Pipeline, Alert.pipeline_id == Pipeline.id
                ).outerjoin(
                    AlertConfig, Alert.alert_config_id == AlertConfig.id
                ).filter(
                    or_(
                        Pipeline.user_id == user_id,
                        AlertConfig.user_id == user_id,
                    )
                ).order_by(desc(Alert.created_at)).limit(5)

                recent_alerts = [
                    DashboardAlert(
                        id=a.id,
                        message=a.message,
                        level=a.level.value,
                        created_at=a.created_at,
                        pipeline_id=a.pipeline_id
                    ) for a in alerts_query.all()
                ]
            except Exception as e:
                self.db.rollback()
                logger.error(f"Error fetching recent alerts: {e}")
                recent_alerts = []

            return DashboardStats(
                total_pipelines=total_pipelines,
                active_pipelines=active_pipelines,
                total_connections=total_connections,
                connector_health=connector_health,
                
                total_jobs=total_jobs,
                success_rate=round(success_rate, 1),
                avg_duration=round(avg_duration, 2),
                total_rows=total_rows,
                total_bytes=total_bytes,
                
                throughput=throughput,
                pipeline_distribution=distribution,
                recent_activity=activity,
                
                system_health=system_health,
                top_failing_pipelines=top_failing,
                slowest_pipelines=slowest_pipelines,
                recent_alerts=recent_alerts
            )
        except Exception as e:
            logger.error("Error generating dashboard stats", error=str(e), exc_info=True)
            raise e