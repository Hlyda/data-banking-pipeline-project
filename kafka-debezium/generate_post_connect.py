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

response = requests.post(url, headers=headers, data=json.dumps(connector_config))

# -----------------------------
# Debug/Output
# -----------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists.")
else:
    print(f"❌ Failed to create connector ({response.status_code}): {response.text}")