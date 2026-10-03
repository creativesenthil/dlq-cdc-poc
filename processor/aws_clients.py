import os
import boto3

AWS_ENDPOINT = os.environ.get("AWS_ENDPOINT", "http://localhost:4566")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")

S3_BUCKET = os.environ.get("S3_BUCKET", "cdc-landing")
SQS_MAIN_QUEUE = os.environ.get("SQS_MAIN_QUEUE", "cdc-main")
SQS_DLQ = os.environ.get("SQS_DLQ", "cdc-dlq")


def _client(service):
    return boto3.client(
        service,
        endpoint_url=AWS_ENDPOINT,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def s3_client():
    return _client("s3")


def sqs_client():
    return _client("sqs")


def dlq_url():
    sqs = sqs_client()
    return sqs.get_queue_url(QueueName=SQS_DLQ)["QueueUrl"]


def main_queue_url():
    sqs = sqs_client()
    return sqs.get_queue_url(QueueName=SQS_MAIN_QUEUE)["QueueUrl"]
