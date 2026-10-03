"""
Default lightweight processor (no Flink runtime required).

Reads Debezium change events off the Kafka/Redpanda topic, validates
each one, and either:
  - writes valid records to S3 (the "landing zone"), or
  - routes malformed records to the SQS Dead Letter Queue, tagged with
    a failure_reason, and keeps running (does not crash the pipeline).

Can also be run directly against a single JSON file for local testing:
    python consumer.py --input ../data/valid_customer.json
    python consumer.py --input ../data/poison_customer.json
"""
import argparse
import json
import os
import sys
import time
import uuid

from validation import validate_record, extract_after, ValidationError
from aws_clients import s3_client, sqs_client, dlq_url, S3_BUCKET

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "dbz.inventory.customers")


def handle_record(raw_envelope: dict, s3, sqs, dlq_queue_url, valid_count, dlq_count):
    record = extract_after(raw_envelope)
    try:
        validated = validate_record(record)
        key = f"customers/id={validated['id']}/{uuid.uuid4()}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(validated).encode("utf-8"),
            ContentType="application/json",
        )
        print(f"OK -> s3://{S3_BUCKET}/{key} (id={validated.get('id')})")
        valid_count[0] += 1
    except ValidationError as e:
        sqs.send_message(
            QueueUrl=dlq_queue_url,
            MessageBody=json.dumps(record),
            MessageAttributes={
                "failure_reason": {"DataType": "String", "StringValue": str(e)}
            },
        )
        print(f"POISON PILL -> DLQ | reason={e}")
        dlq_count[0] += 1
        # Deliberately NOT re-raising: the pipeline must keep processing
        # subsequent messages even after a bad record.


def run_single_file(path):
    with open(path) as f:
        record = json.load(f)
    s3 = s3_client()
    sqs = sqs_client()
    dlq_queue_url = dlq_url()
    valid_count, dlq_count = [0], [0]
    handle_record(record, s3, sqs, dlq_queue_url, valid_count, dlq_count)
    print(f"DONE. valid->S3={valid_count[0]}  malformed->DLQ={dlq_count[0]}")


def run_kafka_loop():
    from kafka import KafkaConsumer

    s3 = s3_client()
    sqs = sqs_client()
    dlq_queue_url = dlq_url()
    valid_count, dlq_count = [0], [0]

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="dlq-poc-processor",
    )
    print(f"Listening on topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP} ...")
    for message in consumer:
        if message.value is None:
            continue
        try:
            handle_record(message.value, s3, sqs, dlq_queue_url, valid_count, dlq_count)
        except Exception as e:
            # Catch-all so one bad message can never kill the loop.
            print(f"UNEXPECTED ERROR (message skipped, not lost - still in Kafka log): {e}")
        print(f"  running totals: valid->S3={valid_count[0]}  malformed->DLQ={dlq_count[0]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Process a single local JSON file instead of Kafka")
    args = parser.parse_args()

    if args.input:
        run_single_file(args.input)
    else:
        run_kafka_loop()
