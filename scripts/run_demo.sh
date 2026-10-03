#!/usr/bin/env bash
# One-command orchestrator: brings up the stack, wires AWS resources and
# Kafka/Debezium, seeds data, and shows the processor's result.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo "=== 1. Bringing up the stack ==="
docker compose up --build -d

echo ""
echo "=== 2. Waiting for containers to settle (20s) ==="
sleep 20

echo ""
echo "=== 3. Creating AWS resources (S3, SQS main + DLQ) ==="
bash scripts/setup_aws.sh

echo ""
echo "=== 4. Creating Kafka topic + registering Debezium connector ==="
bash scripts/setup_kafka.sh

echo ""
echo "=== 5. Restarting processor (so it picks up the now-existing DLQ) ==="
docker restart dlq-processor
sleep 5

echo ""
echo "=== 6. Seeding MongoDB with valid + poison records ==="
bash scripts/seed_mongo.sh

echo ""
echo "=== 7. Giving CDC a few seconds to propagate ==="
sleep 15

echo ""
echo "=== 8. Processor logs (look for OK -> S3 and POISON PILL -> DLQ) ==="
docker logs --tail 50 dlq-processor

echo ""
echo "=== 9. Running remediation against the DLQ ==="
docker exec -i dlq-processor python remediation.py

echo ""
echo "=== Demo complete. Stack is still running. ==="
echo "To tear down: docker compose down -v"
