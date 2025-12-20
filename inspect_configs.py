import sys
import os
import json

# Add the current directory to sys.path to make the app module importable
sys.path.append(os.getcwd())

try:
    from app.connectors.impl.files.local import LocalFileConfig
    from app.connectors.impl.sql.postgres import PostgresConfig
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def print_schema(title, model):
    print(f"\n{'='*10} {title} {'='*10}")
    schema = model.model_json_schema()
    properties = schema.get("properties", {})
    required = schema.get("required", [])
    
    print(f"{ 'Field':<15} {'Type':<10} {'Required':<10} {'Default':<10} {'Description'}")
    print("-" * 100)
    
    for field, details in properties.items():
        is_required = "Yes" if field in required else "No"
        field_type = details.get("type", "any")
        default = str(details.get("default", "-"))
        description = details.get("description", "")
        print(f"{field:<15} {field_type:<10} {is_required:<10} {default:<10} {description}")

if __name__ == "__main__":
    print_schema("Local File Connector Config", LocalFileConfig)
    print_schema("PostgreSQL Connector Config", PostgresConfig)
