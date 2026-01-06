import os
import json
import requests
from dotenv import load_dotenv

# json.dumps() : convert a dictionnary into a json file 
# create an http request with requests.post to post the connector to debezium connect

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

# -----------------------------
# Build connector JSON in memory
# -----------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}
#"decimal.handling.mode": "double" : This setting configures how Debezium handles decimal types from PostgreSQL. By setting it to "double", decimal values will be represented as double-precision floating-point numbers in the Kafka topics. This can be useful for simplifying data processing in downstream applications that consume the Kafka topics, especially if they do not require the full precision of decimal types.


# -----------------------------
# Send request to Debezium Connect
# -----------------------------
url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

try:
    response = requests.post(url, headers=headers, data=json.dumps(connector_config), timeout=5)
    
    # -----------------------------
    # Debug/Output
    # -----------------------------
    if response.status_code == 201:
        print("✅ Connector created successfully!")
    elif response.status_code == 409:
        print("⚠️ Connector already exists.")
    else:
        print(f"❌ Failed to create connector ({response.status_code}): {response.text}")
        
except requests.exceptions.ConnectionError:
    print("❌ Connection Error: Cannot connect to Kafka Connect REST API at http://localhost:8083")
    print("\n💡 Make sure Kafka Connect is running:")
    print("   - If using Docker Compose, run: docker-compose up -d")
    print("   - Check if the service is running: docker ps | grep connect")
    print("   - Verify the service is accessible: curl http://localhost:8083/connectors")
    exit(1)
    
except requests.exceptions.Timeout:
    print("❌ Timeout: Kafka Connect REST API did not respond in time")
    print("💡 The service might be starting up. Please wait a moment and try again.")
    exit(1)
    
except Exception as e:
    print(f"❌ Unexpected error: {type(e).__name__}: {str(e)}")
    exit(1)