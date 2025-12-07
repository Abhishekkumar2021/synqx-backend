#!/bin/bash

# =============================================================================
# SynqX API End-to-End Test Script (Bash + Curl)
# =============================================================================
# Usage: ./test_api_flow.sh
# Prerequisites: Backend running at http://localhost:8000
# =============================================================================

BASE_URL="http://localhost:8000/api/v1"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_DB="synqx_database"
POSTGRES_USER="postgres"
POSTGRES_PASS="postgres"
# Use absolute path for local file connector to be safe
LOCAL_FILE_PATH="$(pwd)/data/files"

# Helper to parse JSON with Python
# Usage: echo $JSON | json_val "['key']"
json_val() {
    python3 -c "import sys, json; print(json.load(sys.stdin)$1)" 2>/dev/null
}

echo "Waiting for backend..."
for i in {1..30}; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{{http_code}}" http://localhost:8000/health)
    if [ "$HTTP_CODE" == "200" ]; then
        echo "Backend is UP."
        break
    fi
    sleep 1
done

if [ "$HTTP_CODE" != "200" ]; then
    echo "Backend not reachable. Exiting."
    exit 1
fi

echo ""
echo "1. Create Source Connection (PostgreSQL)"
# Generate a random suffix to avoid name collision if run multiple times
RAND=$RANDOM
SOURCE_NAME="Postgres_Curl_$RAND"
RESPONSE=$(curl -s -X POST "$BASE_URL/connections" \
  -H "Content-Type: application/json" \
  -d '{'
    "name": "'$SOURCE_NAME'",
    "connector_type": "postgresql",
    "description": "Source DB via Curl",
    "config": {
        "host": "'$POSTGRES_HOST'",
        "port": '$POSTGRES_PORT',
        "database": "'$POSTGRES_DB'",
        "username": "'$POSTGRES_USER'",
        "password": "'$POSTGRES_PASS'",
        "db_schema": "public"
    }
  }')
SOURCE_CONN_ID=$(echo "$RESPONSE" | json_val "['id']")
echo " -> ID: $SOURCE_CONN_ID"

echo ""
echo "2. Create Destination Connection (Local File)"
DEST_NAME="LocalFile_Curl_$RAND"
RESPONSE=$(curl -s -X POST "$BASE_URL/connections" \
  -H "Content-Type: application/json" \
  -d '{'
    "name": "'$DEST_NAME'",
    "connector_type": "local_file",
    "description": "Dest File via Curl",
    "config": {
        "base_path": "'$LOCAL_FILE_PATH'"
    }
  }')
DEST_CONN_ID=$(echo "$RESPONSE" | json_val "['id']")
echo " -> ID: $DEST_CONN_ID"

echo ""
echo "3. Create Source Asset"
RESPONSE=$(curl -s -X POST "$BASE_URL/connections/$SOURCE_CONN_ID/assets" \
  -H "Content-Type: application/json" \
  -d '{'
    "name": "connections",
    "asset_type": "table",
    "is_source": true,
    "connection_id": '$SOURCE_CONN_ID