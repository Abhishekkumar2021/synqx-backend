# SynqX API Quickstart Guide

This guide provides `curl` commands to test the core end-to-end flow of the SynqX ETL engine.

**Prerequisites:**
- Backend running at `http://localhost:8000`
- PostgreSQL database running (as configured in `.env`)
- `jq` installed (optional, for pretty printing JSON)

## 1. System Health
Check if the API and database are ready.
```bash
curl -X GET "http://localhost:8000/health"
```

## 2. Create Connections

### Source (PostgreSQL)
Replace credentials with your actual database configuration.
```bash
curl -X POST "http://localhost:8000/api/v1/connections" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production DB",
    "connector_type": "postgresql",
    "description": "Primary PostgreSQL Database",
    "config": {
        "host": "localhost",
        "port": 5432,
        "database": "synqx_database",
        "username": "postgres",
        "password": "postgres",
        "db_schema": "public"
    }
  }'
```
*Note the `id` from the response (e.g., `1`).*

### Destination (Local File)
```bash
curl -X POST "http://localhost:8000/api/v1/connections" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Local Data Lake",
    "connector_type": "local_file",
    "description": "Local CSV storage",
    "config": {
        "base_path": "./data/files"
    }
  }'
```
*Note the `id` from the response (e.g., `2`).*

## 3. Discover & Define Assets

### Discover Tables (Source)
```bash
curl -X POST "http://localhost:8000/api/v1/connections/1/discover" \
  -H "Content-Type: application/json" \
  -d '{"include_metadata": true}'
```

### Register Source Asset
Register the `connections` table (or any other existing table).
```bash
curl -X POST "http://localhost:8000/api/v1/connections/1/assets" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "connections",
    "asset_type": "table",
    "is_source": true,
    "connection_id": 1
  }'
```
*Note the Asset ID (e.g., `10`).*

### Register Destination Asset
Define the output CSV file.
```bash
curl -X POST "http://localhost:8000/api/v1/connections/2/assets" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "output_data.csv",
    "asset_type": "file",
    "is_destination": true,
    "connection_id": 2
  }'
```
*Note the Asset ID (e.g., `11`).*

## 4. Create Pipeline
Create a pipeline to extract data from Postgres and load it into a CSV file.
```bash
curl -X POST "http://localhost:8000/api/v1/pipelines" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Postgres to CSV",
    "description": "Demo ETL pipeline",
    "initial_version": {
        "nodes": [
            {
                "node_id": "extract",
                "name": "Extract Data",
                "operator_type": "extract",
                "operator_class": "PostgresExtractor",
                "config": {"query": "SELECT * FROM public.connections LIMIT 10"},
                "order_index": 0,
                "source_asset_id": 10
            },
            {
                "node_id": "load",
                "name": "Load CSV",
                "operator_type": "load",
                "operator_class": "LocalFileLoader",
                "config": {"path": "output_data.csv", "mode": "replace"},
                "order_index": 1,
                "destination_asset_id": 11
            }
        ],
        "edges": [
            {"from_node_id": "extract", "to_node_id": "load"}
        ]
    }
  }'
```
*Note the Pipeline ID and Version ID from the response.*

## 5. Trigger Execution
Trigger the pipeline asynchronously.
```bash
curl -X POST "http://localhost:8000/api/v1/pipelines/1/trigger" \
  -H "Content-Type: application/json" \
  -d '{
    "version_id": 1,
    "async_execution": true
  }'
```
*Returns a `job_id`.*

## 6. Monitoring

### Check Job Status
```bash
curl -X GET "http://localhost:8000/api/v1/jobs/1"
```

### Get Execution Logs
```bash
curl -X GET "http://localhost:8000/api/v1/jobs/1/logs"
```

### Pipeline Statistics
```bash
curl -X GET "http://localhost:8000/api/v1/pipelines/1/stats"
```
