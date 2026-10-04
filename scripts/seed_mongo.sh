#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALID=$(cat "$SCRIPT_DIR/../data/valid_customer.json")
POISON=$(cat "$SCRIPT_DIR/../data/poison_customer.json")

echo "Inserting valid record ..."
docker exec -i dlq-mongo mongosh --quiet --eval "
  db.getSiblingDB('inventory').customers.insertOne($VALID);
"

echo "Inserting poison-pill record ..."
docker exec -i dlq-mongo mongosh --quiet --eval "
  db.getSiblingDB('inventory').customers.insertOne($POISON);
"

echo "Both documents inserted. Give CDC a few seconds to propagate."
