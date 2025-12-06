# SynqX - Universal ETL Engine

SynqX is a robust, scalable, and secure backend for a universal ETL (Extract, Transform, Load) agent. It allows users to define data pipelines with flexible source/destination connectors, transformation logic, and scheduling capabilities.

## 🚀 Features

*   **Pipeline Management**: Create, update, and version data pipelines.
*   **DAG Execution Engine**: Topological sorting and execution of complex dependency graphs.
*   **Connectors**: Extensible architecture for data sources and destinations (currently supports PostgreSQL).
*   **Transformations**: Pluggable transformation logic (currently supports Pandas-based transforms).
*   **Scheduling**: Distributed task scheduling using Celery and Redis.
*   **Security**: AES-256 encryption for sensitive connection configurations at rest.
*   **Observability**: Structured JSON logging and database-persisted execution logs (Job & Step levels).

## 🛠️ Prerequisites

*   **Python**: 3.10+
*   **PostgreSQL**: Primary metadata storage and default data source/destination.
*   **Redis**: Message broker for the task queue.

## ⚙️ Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd backend
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Create a `.env` file in the root directory:
    ```bash
    APP_NAME="SynqX ETL Agent"
    ENVIRONMENT="development"
    
    # Database (Ensure this DB exists)
    DATABASE_URL="postgresql://user:password@localhost:5432/synqx_db"
    
    # Redis (For Celery)
    REDIS_URL="redis://localhost:6379/0"
    
    # Security (Critical for config encryption)
    MASTER_PASSWORD="change_this_to_a_secure_random_string"
    
    # Logging
    LOG_LEVEL="INFO"
    JSON_LOGS=False
    ```

## 🏃‍♂️ Running the Application

You need to run three separate processes for the full system to function.

### 1. Backend API Server
This serves the REST API and handles database migrations/seeding on startup.
```bash
uvicorn main:app --reload
```
*On first run, this will automatically create tables and seed a "Default Postgres" connection.*

### 2. Celery Worker
This executes the actual pipeline tasks in the background.
```bash
celery -A app.core.celery_app worker --loglevel=info
```

### 3. Celery Beat (Scheduler)
This triggers scheduled pipelines based on their CRON expressions.
```bash
celery -A app.core.celery_app beat --loglevel=info
```

## 🔄 End-to-End Usage Flow

### Step 1: Verify Setup
Access the API documentation at `http://localhost:8000/docs`.
Check the health endpoint: `GET /health`.

### Step 2: Create a Pipeline
Use the `POST /api/v1/pipelines/` endpoint.
**Example Payload:**
This pipeline reads from the `users` table (source) and writes to the `users_backup` table (destination) using the default seeded Postgres connection.

*Note: Ensure you have `users` and `users_backup` tables in your Postgres DB or update the asset names below.*

```json
{
  "name": "User Backup Pipeline",
  "description": "Backs up user data daily",
  "schedule_cron": "0 0 * * *", 
  "schedule_enabled": true,
  "initial_version": {
    "config_snapshot": {},
    "nodes": [
      {
        "node_id": "extract_users",
        "name": "Extract Users",
        "operator_type": "extract",
        "operator_class": "postgresql",
        "source_asset_id": 1, 
        "order_index": 1
      },
      {
        "node_id": "load_backup",
        "name": "Load Backup",
        "operator_type": "load",
        "operator_class": "postgresql",
        "destination_asset_id": 1,
        "order_index": 2
      }
    ],
    "edges": [
      {
        "from_node_id": "extract_users",
        "to_node_id": "load_backup"
      }
    ]
  }
}
```
*Note: `source_asset_id` and `destination_asset_id` refer to `Asset` records. You might need to create Assets first via DB script or future API, or for the MVP, rely on the default seeded logic if expanded.*

### Step 3: Trigger a Run
Manually trigger the pipeline via API:
`POST /api/v1/pipelines/{pipeline_id}/run`

Response:
```json
{
  "status": "success",
  "message": "Pipeline run completed successfully.",
  "job_id": 1
}
```

### Step 4: Monitor Execution
Check the logs in your terminal or inspect the database tables:
*   **`pipeline_runs`**: High-level status of the run.
*   **`step_runs`**: Detailed metrics (records in/out) for each node.
*   **`job_logs`**: System logs linked to the job.
*   **`step_logs`**: Execution logs specific to each step (e.g., "Reading from source...", "Wrote 100 records").

## 🧪 Testing

Run the test suite using `pytest`:

```bash
pytest
```

## 📂 Project Structure

*   `app/api`: FastAPI route handlers.
*   `app/connectors`: Data source/destination implementations (Postgres, S3, etc.).
*   `app/core`: Configuration, logging, and security.
*   `app/db`: Database session and base models.
*   `app/engine`: Core logic (DAG, Runner, Scheduler, Transforms).
*   `app/models`: SQLAlchemy ORM models.
*   `app/services`: Business logic (PipelineService, VaultService).
*   `app/worker`: Celery task definitions.
