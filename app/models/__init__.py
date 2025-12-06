# Import Base first
from app.models.base import (
    Base, 
    TimestampMixin, 
    UserTrackingMixin, 
    AuditMixin, 
    SoftDeleteMixin
)

from app.models.enums import (
    ConnectorType,
    PipelineStatus,
    PipelineRunStatus,
    OperatorType,
    OperatorRunStatus,
    JobStatus,
    RetryStrategy,
    DataDirection,
    AlertLevel,
    AlertStatus,
    AlertType,
    AlertDeliveryMethod
)

from app.models.connections import Connection, Asset, AssetSchemaVersion
from app.models.pipelines import Pipeline, PipelineVersion, PipelineNode, PipelineEdge
from app.models.execution import Job, PipelineRun, StepRun, PipelineRunContext, Watermark
from app.models.monitoring import SchedulerEvent, JobLog, StepLog, AlertConfig, Alert

# Export all models for Alembic and easy access
__all__ = [
    "Base",
    "TimestampMixin",
    "UserTrackingMixin",
    "AuditMixin",
    "SoftDeleteMixin",
    
    # Enums
    "ConnectorType",
    "PipelineStatus",
    "PipelineRunStatus",
    "OperatorType",
    "OperatorRunStatus",
    "JobStatus",
    "RetryStrategy",
    "DataDirection",
    "AlertLevel",
    "AlertStatus",
    "AlertType",
    "AlertDeliveryMethod",

    # Models
    "Connection",
    "Asset",
    "AssetSchemaVersion",
    "Pipeline",
    "PipelineVersion",
    "PipelineNode",
    "PipelineEdge",
    "Job",
    "PipelineRun",
    "StepRun",
    "PipelineRunContext",
    "Watermark",
    "SchedulerEvent",
    "JobLog",
    "StepLog",
    "AlertConfig",
    "Alert",
]