"""
Optional real Flink job, sharing the same validation core as consumer.py.
Only used when the stack is brought up with `--profile flink`.

This is intentionally minimal: a single-pipeline job reading from Kafka
and invoking the same validate-or-DLQ logic via a map function.

Run inside the Flink cluster (jobmanager/taskmanager containers):
    flink run -py flink_job.py
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json
import os

from validation import validate_record, extract_after, ValidationError
from aws_clients import s3_client, sqs_client, dlq_url, S3_BUCKET

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "redpanda:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "dbz.inventory.customers")


def process(value: str):
    s3 = s3_client()
    sqs = sqs_client()
    dlq_queue_url = dlq_url()

    envelope = json.loads(value)
    record = extract_after(envelope)
    try:
        validated = validate_record(record)
        key = f"customers/id={validated['id']}/{validated['id']}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(validated).encode("utf-8"),
        )
        return f"OK:{validated['id']}"
    except ValidationError as e:
        sqs.send_message(
            QueueUrl=dlq_queue_url,
            MessageBody=json.dumps(record),
            MessageAttributes={"failure_reason": {"DataType": "String", "StringValue": str(e)}},
        )
        return f"DLQ:{e}"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = FlinkKafkaConsumer(
        topics=KAFKA_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties={"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "dlq-poc-flink"},
    )

    stream = env.add_source(kafka_source)
    result = stream.map(process, output_type=Types.STRING())
    result.print()

    env.execute("dlq-cdc-poc-flink-job")


if __name__ == "__main__":
    main()
