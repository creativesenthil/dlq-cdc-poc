"""
Reads messages off the DLQ, logs the failure reason, and attempts a
conservative auto-repair. If repair succeeds, the record is replayed to S3
and removed from the DLQ. If repair isn't possible, the message is left on
the DLQ (visible, not lost) for a human to inspect.

Run manually:
    python remediation.py
"""
import json
import uuid

from validation import validate_record, ValidationError
from aws_clients import s3_client, sqs_client, dlq_url, S3_BUCKET


def try_repair(record: dict) -> dict:
    """
    Conservative, explicit repairs only - never silently invents data.
    Currently handles exactly one benign defect: 'id' arriving as a
    numeric string (e.g. "2002") instead of an int.
    """
    repaired = dict(record)
    if "id" in repaired and isinstance(repaired["id"], str) and repaired["id"].isdigit():
        repaired["id"] = int(repaired["id"])
    return repaired


def run_remediation():
    s3 = s3_client()
    sqs = sqs_client()
    queue_url = dlq_url()

    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        MessageAttributeNames=["All"],
        WaitTimeSeconds=1,
    )
    messages = response.get("Messages", [])
    if not messages:
        print("DLQ is empty - nothing to remediate.")
        return

    for msg in messages:
        body = json.loads(msg["Body"])
        reason = (
            msg.get("MessageAttributes", {})
            .get("failure_reason", {})
            .get("StringValue", "unknown reason")
        )
        print(f"DLQ message: {body}  | failure_reason={reason}")

        repaired = try_repair(body)
        try:
            validated = validate_record(repaired)
            key = f"customers/id={validated['id']}/replayed-{uuid.uuid4()}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(validated).encode("utf-8"),
                ContentType="application/json",
            )
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            print(f"  -> REPAIRED and replayed to s3://{S3_BUCKET}/{key}, removed from DLQ")
        except ValidationError as e:
            print(f"  -> NOT auto-recoverable ({e}); left on DLQ for manual review")


if __name__ == "__main__":
    run_remediation()
