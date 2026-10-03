#!/usr/bin/env bash
# Creates the S3 bucket and SQS main queue + DLQ (with redrive policy)
# inside LocalStack. Idempotent - safe to re-run.
set -euo pipefail

ENDPOINT="${AWS_ENDPOINT:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_REGION:-us-east-1}"

echo "Waiting for LocalStack to be ready at $ENDPOINT ..."
until curl -sf "$ENDPOINT/_localstack/health" > /dev/null; do
  sleep 2
done

echo "Creating S3 bucket: cdc-landing"
aws --endpoint-url="$ENDPOINT" s3 mb s3://cdc-landing 2>/dev/null || echo "  (already exists)"

echo "Creating DLQ: cdc-dlq"
DLQ_URL=$(aws --endpoint-url="$ENDPOINT" sqs create-queue --queue-name cdc-dlq --query 'QueueUrl' --output text)
DLQ_ARN=$(aws --endpoint-url="$ENDPOINT" sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
echo "  DLQ URL: $DLQ_URL"
echo "  DLQ ARN: $DLQ_ARN"

echo "Creating main queue: cdc-main (with redrive policy -> cdc-dlq, maxReceiveCount=3)"
REDRIVE_POLICY=$(printf '{"deadLetterTargetArn":"%s","maxReceiveCount":"3"}' "$DLQ_ARN")
aws --endpoint-url="$ENDPOINT" sqs create-queue \
  --queue-name cdc-main \
  --attributes "{\"RedrivePolicy\":\"$(echo "$REDRIVE_POLICY" | sed 's/"/\\"/g')\"}" \
  > /dev/null

echo "AWS resources ready: s3://cdc-landing, cdc-main, cdc-dlq"
