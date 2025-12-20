import sys
import os
import shutil
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.getcwd())

from app.models.base import Base
from app.models.user import User
from app.models.connections import Connection, Asset
from app.models.enums import OperatorType, ConnectorType
from app.schemas.pipeline import (
    PipelineCreate, PipelineVersionCreate, PipelineNodeCreate, PipelineEdgeCreate
)
from app.services.pipeline_service import PipelineService
from app.services.vault_service import VaultService
from app.engine.runner import PipelineRunner
from app.core.config import settings

# Setup Test DB
SQLALCHEMY_DATABASE_URL = "sqlite:///./test_e2e.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def create_dummy_data(db):
    # 1. Create User
    user = User(
        email="test@example.com",
        full_name="Test User",
        hashed_password="fake_hash",
        is_active=True
    )
    db.add(user)
    db.flush()

    # 2. Create Connectors (Using Local File for simplicity)
    # Source Config
    source_config = {"base_path": "./data/files"}
    encrypted_source = VaultService.encrypt_config(source_config)
    
    source_conn = Connection(
        name="Source Local File",
        connector_type=ConnectorType.LOCAL_FILE,
        config_encrypted=encrypted_source,
        user_id=user.id
    )
    db.add(source_conn)
    db.flush()

    # Destination Config
    dest_config = {"base_path": "./data/files_out"}
    encrypted_dest = VaultService.encrypt_config(dest_config)
    
    dest_conn = Connection(
        name="Dest Local File",
        connector_type=ConnectorType.LOCAL_FILE,
        config_encrypted=encrypted_dest,
        user_id=user.id
    )
    db.add(dest_conn)
    db.flush()

    # 3. Create Assets
    source_asset = Asset(
        name="input.csv",
        connection_id=source_conn.id,
        asset_type="csv",
        config={"file_path": "input.csv", "file_type": "csv"}
    )
    db.add(source_asset)
    
    dest_asset = Asset(
        name="output.csv",
        connection_id=dest_conn.id,
        asset_type="csv",
        config={"file_path": "output.csv", "file_type": "csv"}
    )
    db.add(dest_asset)
    db.commit()
    
    return user, source_asset, dest_asset

def create_input_file():
    os.makedirs("./data/files", exist_ok=True)
    os.makedirs("./data/files_out", exist_ok=True)
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [25, 30, 35]})
    df.to_csv("./data/files/input.csv", index=False)
    print("Created ./data/files/input.csv")

def test_pipeline_flow():
    init_db()
    
    # Clear output directory before each test run
    output_dir = "./data/files_out"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    create_input_file()
    
    db = TestingSessionLocal()
    try:
        user, source_asset, dest_asset = create_dummy_data(db)
        
        # 4. Create Pipeline Definition
        nodes = [
            PipelineNodeCreate(
                node_id="source_node",
                name="Read CSV",
                operator_type=OperatorType.EXTRACT, # Changed from SOURCE to EXTRACT
                operator_class="local_file_reader", # This needs to be a real class
                order_index=0,
                source_asset_id=source_asset.id
            ),
            PipelineNodeCreate(
                node_id="transform_node",
                name="Filter Age",
                operator_type=OperatorType.TRANSFORM,
                operator_class="filter",
                config={"condition": "age > 28"},
                order_index=1
            ),
            PipelineNodeCreate(
                node_id="dest_node",
                name="Write CSV",
                operator_type=OperatorType.LOAD, # Changed from SINK to LOAD
                operator_class="local_file_writer", # This needs to be a real class
                order_index=2,
                destination_asset_id=dest_asset.id
            )
        ]
        
        edges = [
            PipelineEdgeCreate(from_node_id="source_node", to_node_id="transform_node"),
            PipelineEdgeCreate(from_node_id="transform_node", to_node_id="dest_node")
        ]
        
        pipeline_create = PipelineCreate(
            name="E2E Test Pipeline",
            initial_version=PipelineVersionCreate(
                nodes=nodes,
                edges=edges
            )
        )
        
        service = PipelineService(db)
        pipeline = service.create_pipeline(pipeline_create, user_id=user.id)
        print(f"Pipeline created: {pipeline.id}")
        
        # Correct approach:
        # service.create_pipeline creates a version.
        # Let's get the version ID.
        from app.models.pipelines import PipelineVersion
        version = db.query(PipelineVersion).filter_by(pipeline_id=pipeline.id, version=1).first()
        
        print(f"Attempting to trigger run for version ID: {version.id}") # Added print for debugging
        result = service.trigger_pipeline_run(
            pipeline_id=pipeline.id,
            version_id=version.id,
            async_execution=False,
            user_id=user.id
        )
        
        print(f"Run Result: {result}")
        
        if result['status'] == 'success':
            print("Pipeline executed successfully!")
            # Verify output
            if os.path.exists("./data/files_out/output.csv"):
                out_df = pd.read_csv("./data/files_out/output.csv")
                print("Output Data:")
                print(out_df)
                if len(out_df) == 2: # Bob and Charlie
                    print("TEST PASSED: Correct number of rows.")
                else:
                    print(f"TEST FAILED: Expected 2 rows, got {len(out_df)}")
            else:
                print("TEST FAILED: Output file not found.")
        else:
            print("TEST FAILED: Pipeline execution failed.")
            
    except Exception as e:
        print(f"TEST FAILED with Exception: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    test_pipeline_flow()
