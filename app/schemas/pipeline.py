from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, conint, Json
from app.models.enums import PipelineStatus, OperatorType

# Schemas for Pipeline Nodes and Edges
class PipelineNodeCreate(BaseModel):
    node_id: str = Field(..., description="Unique identifier for the node within the pipeline version.")
    name: str = Field(..., description="Display name of the node.")
    description: Optional[str] = None
    operator_type: OperatorType = Field(..., description="The type of operation this node performs (e.g., EXTRACT, TRANSFORM, LOAD).")
    operator_class: str = Field(..., description="The specific class implementing the operator (e.g., 'PostgresConnector', 'PythonTransform').")
    config: Dict[str, Any] = Field({}, description="Configuration specific to this operator.")
    order_index: conint(ge=0) = Field(..., description="Order of the node for display or initial processing.")
    source_asset_id: Optional[int] = Field(None, description="ID of the source asset if this is an EXTRACT or TRANSFORM node.")
    destination_asset_id: Optional[int] = Field(None, description="ID of the destination asset if this is a LOAD or TRANSFORM node.")
    max_retries: conint(ge=0) = Field(3, description="Maximum number of retries for this node on failure.")
    timeout_seconds: Optional[conint(gt=0)] = Field(None, description="Timeout for this node's execution in seconds.")

class PipelineNodeRead(PipelineNodeCreate):
    id: int
    pipeline_version_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

class PipelineEdgeCreate(BaseModel):
    from_node_id: str = Field(..., description="The `node_id` of the upstream node.")
    to_node_id: str = Field(..., description="The `node_id` of the downstream node.")
    edge_type: str = Field("data_flow", description="Type of the dependency (e.g., 'data_flow', 'control_flow').")

class PipelineEdgeRead(PipelineEdgeCreate):
    id: int
    pipeline_version_id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# Schemas for Pipeline Versions
class PipelineVersionCreate(BaseModel):
    config_snapshot: Dict[str, Any] = Field(..., description="A snapshot of the pipeline's overall configuration for this version.")
    change_summary: Optional[Json] = Field(None, description="Summary of changes in this version.")
    version_notes: Optional[str] = None
    nodes: List[PipelineNodeCreate] = Field([], description="List of nodes in this pipeline version.")
    edges: List[PipelineEdgeCreate] = Field([], description="List of edges defining dependencies between nodes.")

class PipelineVersionRead(PipelineVersionCreate):
    id: int
    pipeline_id: int
    version: int
    is_published: bool
    published_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# Schemas for Pipelines
class PipelineCreate(BaseModel):
    name: str = Field(..., max_length=255, description="Unique name of the pipeline.")
    description: Optional[str] = None
    schedule_cron: Optional[str] = Field(None, description="CRON expression for scheduling.")
    schedule_enabled: bool = False
    schedule_timezone: str = Field("UTC", description="Timezone for scheduling.")
    max_parallel_runs: conint(gt=0) = Field(1, description="Maximum number of parallel runs for this pipeline.")
    execution_timeout_seconds: Optional[conint(gt=0)] = Field(None, description="Default timeout for pipeline execution in seconds.")
    tags: Optional[Dict[str, Any]] = None
    priority: conint(ge=1, le=10) = Field(5, description="Priority of the pipeline (1-10).")
    
    initial_version: PipelineVersionCreate = Field(..., description="The initial version of the pipeline to create.")

class PipelineUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=255)
    description: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: Optional[bool] = None
    schedule_timezone: Optional[str] = None
    status: Optional[PipelineStatus] = None # Only allowing status update here for simplicity.
    max_parallel_runs: Optional[conint(gt=0)] = None
    execution_timeout_seconds: Optional[conint(gt=0)] = None
    tags: Optional[Dict[str, Any]] = None
    priority: Optional[conint(ge=1, le=10)] = None

class PipelineRead(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    schedule_cron: Optional[str] = None
    schedule_enabled: bool
    schedule_timezone: str
    status: PipelineStatus
    current_version: Optional[int]
    published_version_id: Optional[int]
    max_parallel_runs: int
    execution_timeout_seconds: Optional[int]
    tags: Optional[Dict[str, Any]] = None
    priority: int
    created_at: datetime
    updated_at: datetime
    tenant_id: int # Assuming tenant_id is part of the base model

    # Optional: Include current version details when reading a pipeline
    current_version_detail: Optional[PipelineVersionRead] = None 

    class Config:
        from_attributes = True
