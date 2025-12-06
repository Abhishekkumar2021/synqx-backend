from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from croniter import croniter
from app.models.enums import PipelineStatus, OperatorType


class PipelineNodeBase(BaseModel):
    node_id: str = Field(..., min_length=1, max_length=255)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=2000)
    operator_type: OperatorType
    operator_class: str = Field(..., min_length=1, max_length=255)
    config: Dict[str, Any] = Field(default_factory=dict)
    order_index: int = Field(..., ge=0)
    source_asset_id: Optional[int] = Field(None, gt=0)
    destination_asset_id: Optional[int] = Field(None, gt=0)
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, gt=0, le=86400)

    @field_validator("node_id")
    @classmethod
    def validate_node_id(cls, v: str) -> str:
        if not v.replace("_", "").replace("-", "").isalnum():
            raise ValueError(
                "node_id must contain only alphanumeric characters, hyphens, and underscores"
            )
        return v


class PipelineNodeCreate(PipelineNodeBase):
    pass


class PipelineNodeUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=2000)
    config: Optional[Dict[str, Any]] = None
    max_retries: Optional[int] = Field(None, ge=0, le=10)
    timeout_seconds: Optional[int] = Field(None, gt=0, le=86400)


class PipelineNodeRead(PipelineNodeBase):
    id: int
    pipeline_version_id: int
    tenant_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineEdgeBase(BaseModel):
    from_node_id: str = Field(..., min_length=1, max_length=255)
    to_node_id: str = Field(..., min_length=1, max_length=255)
    edge_type: str = Field(default="data_flow", max_length=50)

    @model_validator(mode="after")
    def validate_no_self_loop(self):
        if self.from_node_id == self.to_node_id:
            raise ValueError(
                "Self-loops are not allowed: from_node_id cannot equal to_node_id"
            )
        return self


class PipelineEdgeCreate(PipelineEdgeBase):
    pass


class PipelineEdgeRead(PipelineEdgeBase):
    id: int
    pipeline_version_id: int
    from_node_id: int
    to_node_id: int
    tenant_id: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineVersionBase(BaseModel):
    config_snapshot: Dict[str, Any] = Field(default_factory=dict)
    change_summary: Optional[Dict[str, Any]] = None
    version_notes: Optional[str] = Field(None, max_length=5000)


class PipelineVersionCreate(PipelineVersionBase):
    nodes: List[PipelineNodeCreate] = Field(default_factory=list, min_length=1)
    edges: List[PipelineEdgeCreate] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_nodes_and_edges(self):
        if not self.nodes:
            raise ValueError("Pipeline version must have at least one node")

        node_ids = {node.node_id for node in self.nodes}

        if len(node_ids) != len(self.nodes):
            raise ValueError("Duplicate node_id values are not allowed")

        for edge in self.edges:
            if edge.from_node_id not in node_ids:
                raise ValueError(
                    f"Edge references non-existent from_node_id: {edge.from_node_id}"
                )
            if edge.to_node_id not in node_ids:
                raise ValueError(
                    f"Edge references non-existent to_node_id: {edge.to_node_id}"
                )

        return self


class PipelineVersionRead(PipelineVersionBase):
    id: int
    pipeline_id: int
    version: int
    is_published: bool
    published_at: Optional[datetime]
    tenant_id: str
    created_at: datetime
    updated_at: datetime
    nodes: List[PipelineNodeRead] = Field(default_factory=list)
    edges: List[PipelineEdgeRead] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class PipelineVersionSummary(BaseModel):
    id: int
    version: int
    is_published: bool
    published_at: Optional[datetime]
    node_count: int
    edge_count: int
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class PipelineBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=5000)
    schedule_cron: Optional[str] = Field(None, max_length=100)
    schedule_enabled: bool = Field(default=False)
    schedule_timezone: str = Field(default="UTC", max_length=50)
    max_parallel_runs: int = Field(default=1, ge=1, le=100)
    execution_timeout_seconds: Optional[int] = Field(None, gt=0, le=86400)
    tags: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)

    @field_validator("schedule_cron")
    @classmethod
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                croniter(v)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {str(e)}")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Pipeline name cannot be empty or only whitespace")
        return v.strip()


class PipelineCreate(PipelineBase):
    initial_version: PipelineVersionCreate

    @model_validator(mode="after")
    def validate_schedule(self):
        if self.schedule_enabled and not self.schedule_cron:
            raise ValueError("schedule_cron is required when schedule_enabled is True")
        return self


class PipelineUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=5000)
    schedule_cron: Optional[str] = Field(None, max_length=100)
    schedule_enabled: Optional[bool] = None
    schedule_timezone: Optional[str] = Field(None, max_length=50)
    status: Optional[PipelineStatus] = None
    max_parallel_runs: Optional[int] = Field(None, ge=1, le=100)
    execution_timeout_seconds: Optional[int] = Field(None, gt=0, le=86400)
    tags: Optional[Dict[str, Any]] = None
    priority: Optional[int] = Field(None, ge=1, le=10)

    @field_validator("schedule_cron")
    @classmethod
    def validate_cron(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            try:
                croniter(v)
            except Exception as e:
                raise ValueError(f"Invalid cron expression: {str(e)}")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and not v.strip():
            raise ValueError("Pipeline name cannot be empty or only whitespace")
        return v.strip() if v else v


class PipelineRead(PipelineBase):
    id: int
    status: PipelineStatus
    current_version: Optional[int]
    published_version_id: Optional[int]
    tenant_id: str
    created_at: datetime
    updated_at: datetime
    deleted_at: Optional[datetime] = None

    model_config = ConfigDict(from_attributes=True)


class PipelineDetailRead(PipelineRead):
    published_version: Optional[PipelineVersionRead] = None
    versions: List[PipelineVersionSummary] = Field(default_factory=list)


class PipelineListResponse(BaseModel):
    pipelines: List[PipelineRead]
    total: int
    limit: int
    offset: int


class PipelineTriggerRequest(BaseModel):
    version_id: Optional[int] = None
    run_params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    async_execution: bool = Field(default=True)


class PipelineTriggerResponse(BaseModel):
    status: str
    message: str
    job_id: int
    task_id: Optional[str] = None
    pipeline_id: int
    version_id: int


class PipelinePublishRequest(BaseModel):
    version_notes: Optional[str] = Field(None, max_length=5000)


class PipelinePublishResponse(BaseModel):
    message: str
    version_id: int
    version_number: int
    published_at: datetime


class PipelineValidationError(BaseModel):
    field: str
    message: str
    error_type: str


class PipelineValidationResponse(BaseModel):
    valid: bool
    errors: List[PipelineValidationError] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class PipelineStatsResponse(BaseModel):
    pipeline_id: int
    total_runs: int
    successful_runs: int
    failed_runs: int
    average_duration_seconds: Optional[float]
    last_run_at: Optional[datetime]
    next_scheduled_run: Optional[datetime]
