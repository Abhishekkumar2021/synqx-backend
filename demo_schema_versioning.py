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

def modify_demo_file(base_path):
    # Modify the CSV file (Change Schema)
    # Changed 'price' to string (currency format) -> Breaking Change? Or just Type Change.
    # Added 'category' column.
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "product": ["Widget A", "Widget B", "Widget C"],
        "price": ["$10.50", "$20.00", "$15.75"], # Changed type to Object/String
        "in_stock": [True, False, True],
        "category": ["Gadget", "Gadget", "Gadget"] # New Column
    })
    df.to_csv(f"{base_path}/products.csv", index=False)
    print(f"\nModified demo file at {base_path}/products.csv (Schema Change)")

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
        conn_create = ConnectionCreate(
            name="Demo Local Files",
            connector_type=ConnectorType.LOCAL_FILE,
            config={"base_path": base_path}
        )
        connection = service.create_connection(conn_create, user_id=user.id)
        
        # 3. CREATE ASSET (Skip discovery for brevity)
        print("\n=== STEP 3: Create Asset ===")
        asset_create = AssetCreate(
            connection_id=connection.id,
            name="products.csv",
            asset_type="csv",
            is_source=True
        )
        asset = service.create_asset(asset_create, user_id=user.id)
        
        # 4. INFER SCHEMA (Version 1)
        print("\n=== STEP 4: Infer Schema (Version 1) ===")
        res1 = service.discover_schema(asset.id, user_id=user.id)
        print(f"Version: {res1.schema_version}")
        print(f"Columns: {[c['name'] for c in res1.discovered_schema['columns']]}")

        # 5. MODIFY DATA & INFER SCHEMA (Version 2)
        modify_demo_file(base_path)
        
        print("\n=== STEP 5: Infer Schema (Version 2) ===")
        res2 = service.discover_schema(asset.id, user_id=user.id)
        
        if res2.success:
            print(f"Version: {res2.schema_version}")
            print(f"Message: {res2.message}")
            print(f"Breaking Change: {res2.is_breaking_change}")
            print("\nNew Schema Columns:")
            print(f"{'Column':<15} {'Type':<10}")
            print("-" * 30)
            for col in res2.discovered_schema.get("columns", []):
                print(f"{col['name']:<15} {col['type']:<10}")
        else:
            print(f"Failed: {res2.message}")

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    run_workflow()
