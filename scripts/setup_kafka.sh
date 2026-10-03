#!/usr/bin/env bash
# Creates the Kafka topic and registers the Debezium MongoDB connector.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Waiting for Debezium Connect REST API ..."
until curl -sf http://localhost:8083/connectors > /dev/null; do
  sleep 2
done

echo "Registering Debezium MongoDB connector ..."
curl -s -X POST -H "Content-Type: application/json" \
  --data @"$SCRIPT_DIR/../connector/mongo-source.json" \
  http://localhost:8083/connectors | python3 -m json.tool || true

echo ""
echo "Connector status:"
curl -s http://localhost:8083/connectors/mongo-source-connector/status | python3 -m json.tool || true
