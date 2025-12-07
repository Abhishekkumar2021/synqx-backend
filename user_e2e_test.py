import httpx
import time
import sys
import os
import json

# Configuration
API_URL = "http://localhost:8000/api/v1"
FILES_OUTPUT_DIR = "./data/files"

# Ensure output directory exists
os.makedirs(FILES_OUTPUT_DIR, exist_ok=True)

def log(msg, data=None):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")
    if data:
        print(json.dumps(data, indent=2))

def wait_for_backend():
    log("Waiting for backend to be ready...")
    for _ in range(30):
        try:
            response = httpx.get("http://localhost:8000/health")
            if response.status_code == 200:
                log("Backend is ready.")
                return True
        except httpx.RequestError:
            pass
        time.sleep(1)
    log("Backend failed to start.")
    return False

def main():
    if not wait_for_backend():
        sys.exit(1)

    # ---------------------------------------------------------
    # 1. Create Source Connection (Postgres)
    # ---------------------------------------------------------
    log("Step 1: Creating Source Connection (Postgres)...")
    pg_config = {
        "name": "Source Postgres",
        "connector_type": "postgresql",
        "description": "Production DB Replica",
        "config": {
            "host": "localhost",
            "port": 5432,
            "database": "synqx_database",
            "username": "postgres",
            "password": "postgres",
            "db_schema": "public"
        }
    }
    
    resp = httpx.post(f"{API_URL}/connections", json=pg_config)
    if resp.status_code != 201:
        log("Failed to create source connection", resp.json())
        sys.exit(1)
    
    source_conn_id = resp.json()["id"]
    log(f"Source Connection Created (ID: {source_conn_id})")

    # ---------------------------------------------------------
    # 2. Create Destination Connection (Local File)
    # ---------------------------------------------------------
    log("Step 2: Creating Destination Connection (Local File)...")
    fs_config = {
        "name": "Data Lake (Local)",
        "connector_type": "local_file",
        "description": "Local CSV staging area",
        "config": {
            "base_path": FILES_OUTPUT_DIR
        }
    }

    resp = httpx.post(f"{API_URL}/connections", json=fs_config)
    if resp.status_code != 201:
        log("Failed to create destination connection", resp.json())
        sys.exit(1)
    
    dest_conn_id = resp.json()["id"]
    log(f"Destination Connection Created (ID: {dest_conn_id})")

    # ---------------------------------------------------------
    # 3. Define Assets (Source & Dest)
    # ---------------------------------------------------------
    log("Step 3: Defining Assets...")
    
    # Corrected: Use a table that actually exists in the 'public' schema
    # 'connections' table is created by the backend so it should exist.
    source_asset_payload = {
        "name": "connections", 
        "asset_type": "table",
        "is_source": True,
        "connection_id": source_conn_id
    }
    resp = httpx.post(f"{API_URL}/connections/{source_conn_id}/assets", json=source_asset_payload)
    if resp.status_code != 201:
        log("Failed to create source asset", resp.json())
        sys.exit(1)
    source_asset_id = resp.json()["id"]

    dest_asset_payload = {
        "name": "user_flow_export.csv",
        "asset_type": "file",
        "is_destination": True,
        "connection_id": dest_conn_id
    }
    resp = httpx.post(f"{API_URL}/connections/{dest_conn_id}/assets", json=dest_asset_payload)
    if resp.status_code != 201:
        log("Failed to create destination asset", resp.json())
        sys.exit(1)
    dest_asset_id = resp.json()["id"]
    
    log(f"Assets Created: Source={source_asset_id}, Dest={dest_asset_id}")

    # ---------------------------------------------------------
    # 4. Create Pipeline (Extract -> Load)
    # ---------------------------------------------------------
    log("Step 4: Creating Data Pipeline...")
    
    pipeline_payload = {
        "name": "User E2E Test Pipeline",
        "description": "Extracts data from Postgres and loads to CSV",
        "tags": {"env": "test", "owner": "user"},
        "initial_version": {
            "nodes": [
                {
                    "node_id": "extract_node",
                    "name": "Extract Data",
                    "operator_type": "extract",
                    "operator_class": "PostgresExtractor",
                    # The query here is optional if the asset is a table, but we can provide one.
                    # PostgresConnector.read_batch uses 'asset' name if not overridden,
                    # OR we can try to see if 'config' overrides. 
                    # Based on code read_batch(asset=...) uses query=SELECT * FROM schema.asset
                    # So this config query might be ignored unless the Connector logic uses it.
                    # Checking PostgresConnector.read_batch again: it constructs query from asset name.
                    # It DOES NOT seem to use 'kwargs' to override query logic in the snippet I read.
                    # So the asset name MUST be the table name.
                    "config": {},
                    "order_index": 0,
                    "source_asset_id": source_asset_id
                },
                {
                    "node_id": "load_node",
                    "name": "Save to CSV",
                    "operator_type": "load",
                    "operator_class": "LocalFileLoader",
                    "config": {"path": "user_flow_export.csv", "mode": "replace"},
                    "order_index": 1,
                    "destination_asset_id": dest_asset_id
                }
            ],
            "edges": [
                {"from_node_id": "extract_node", "to_node_id": "load_node"}
            ]
        }
    }

    resp = httpx.post(f"{API_URL}/pipelines", json=pipeline_payload)
    if resp.status_code != 201:
        log("Failed to create pipeline", resp.json())
        sys.exit(1)
    
    pipeline_data = resp.json()
    pipeline_id = pipeline_data["id"]
    version_id = pipeline_data["versions"][0]["id"]
    log(f"Pipeline Created (ID: {pipeline_id}, Version: {version_id})")

    # ---------------------------------------------------------
    # 5. Trigger Pipeline (Async)
    # ---------------------------------------------------------
    log("Step 5: Triggering Pipeline Execution...")
    
    trigger_payload = {
        "version_id": version_id,
        "async_execution": True
    }
    
    resp = httpx.post(f"{API_URL}/pipelines/{pipeline_id}/trigger", json=trigger_payload)
    if resp.status_code != 200:
        log("Failed to trigger pipeline", resp.json())
        sys.exit(1)
        
    job_id = resp.json()["job_id"]
    log(f"Pipeline Triggered! Job ID: {job_id}")

    # ---------------------------------------------------------
    # 6. Poll Job Status
    # ---------------------------------------------------------
    log("Step 6: Monitoring Job Progress...")
    
    status = "pending"
    start_time = time.time()
    
    while status in ["pending", "queued", "running", "initializing"]:
        if time.time() - start_time > 60:
            log("Timeout waiting for job completion.")
            break
            
        resp = httpx.get(f"{API_URL}/jobs")
        jobs = resp.json()["jobs"]
        current_job = next((j for j in jobs if j["id"] == job_id), None)
        
        if current_job:
            status = current_job["status"].lower()
            print(f"   - Job Status: {status.upper()}")
            if status in ["success", "failed", "cancelled"]:
                if status == "failed":
                    log("Job Failed Details:", current_job)
                break
        
        time.sleep(2)

    # ---------------------------------------------------------
    # 7. Verify Output
    # ---------------------------------------------------------
    log("Step 7: Verifying Results...")
    
    output_path = os.path.join(FILES_OUTPUT_DIR, "user_flow_export.csv")
    if os.path.exists(output_path) and status == "success":
        file_size = os.path.getsize(output_path)
        log(f"Success! Output file exists: {output_path}")
        log(f"File Size: {file_size} bytes")
        with open(output_path, "r") as f:
            content = f.read()
            log("--- File Content Preview (first 200 chars) ---")
            print(content[:200])
            log("----------------------------")
    else:
        if status != "success":
             log(f"Pipeline run failed with status: {status}")
        else:
             log(f"Failure! Output file NOT found at {output_path}")
        sys.exit(1)

    log("End-to-End User Flow Test Completed Successfully.")

if __name__ == "__main__":
    main()