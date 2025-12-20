import sys
import os
import shutil
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add project root to path
sys.path.append(os.getcwd())

from app.models.base import Base
from app.models.user import User
from app.schemas.connection import ConnectionCreate, AssetCreate
from app.services.connection_service import ConnectionService
from app.models.enums import ConnectorType
from app.core.config import settings
import app.connectors.impl # Register connectors

# Setup Test DB
SQLALCHEMY_DATABASE_URL = "sqlite:///./demo_workflow.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def create_demo_files():
    base_path = "./data/demo_files"
    if os.path.exists(base_path):
        shutil.rmtree(base_path)
    os.makedirs(base_path, exist_ok=True)
    
    # Create a CSV file
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "product": ["Widget A", "Widget B", "Widget C"],
        "price": [10.50, 20.00, 15.75],
        "in_stock": [True, False, True]
    })
    df.to_csv(f"{base_path}/products.csv", index=False)
    print(f"Created demo file at {base_path}/products.csv")
    return base_path

def run_workflow():
    init_db()
    base_path = create_demo_files()
    db = SessionLocal()
    
    try:
        # 0. Create User
        user = User(
            email="demo@example.com", 
            hashed_password="dummy_hash",
            is_active=True
        )
        db.add(user)
        db.commit()
        
        service = ConnectionService(db)
        
        # 1. CREATE CONNECTION
        print("\n=== STEP 1: Create Connection ===")
        # Represents POST /connections
        conn_create = ConnectionCreate(
            name="Demo Local Files",
            connector_type=ConnectorType.LOCAL_FILE,
            config={"base_path": base_path},
            description="A connection to local demo files"
        )
        connection = service.create_connection(conn_create, user_id=user.id)
        print(f"Created Connection: ID={connection.id}, Name='{connection.name}'")
        print(f"Health Status: {connection.health_status}")
        
        # 2. DISCOVER ASSETS (Runtime)
        print("\n=== STEP 2: Discover Assets (Runtime Scan) ===")
        # Represents POST /connections/{id}/discover
        discovery = service.discover_assets(connection.id, include_metadata=True)
        print(f"Discovered {discovery.discovered_count} assets in the directory:")
        
        if not discovery.assets:
            print("No assets found! Exiting.")
            return

        for asset_info in discovery.assets:
            print(f" - Found: {asset_info['name']} (Type: {asset_info.get('type')})")

        # Pick the first one to persist
        target_info = discovery.assets[0]
        
        # 3. CREATE ASSET (Persist to DB)
        print("\n=== STEP 3: Create Asset (Persist to DB) ===")
        # Represents POST /connections/{id}/assets
        asset_create = AssetCreate(
            connection_id=connection.id,
            name=target_info["name"],
            asset_type=str(target_info.get("type", "unknown")),
            description="Imported from auto-discovery",
            is_source=True
        )
        asset = service.create_asset(asset_create, user_id=user.id)
        print(f"Persisted Asset: ID={asset.id}, Name='{asset.name}'")
        
        # 4. INFER SCHEMA
        print("\n=== STEP 4: Infer Schema (Version Control) ===")
        # Represents POST /connections/{id}/assets/{id}/discover-schema
        schema_result = service.discover_schema(asset.id, user_id=user.id)
        
        if schema_result.success:
            print(f"Schema Successfully Inferred!")
            print(f"Created Schema Version: {schema_result.schema_version}")
            
            schema = schema_result.discovered_schema
            print("\nColumns Detected:")
            print(f"{ 'Column':<15} { 'Type':<10}")
            print("-" * 30)
            for col in schema.get("columns", []):
                print(f"{col['name']:<15} {col['type']:<10}")
        else:
            print(f"Schema Inference Failed: {schema_result.message}")
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    run_workflow()
