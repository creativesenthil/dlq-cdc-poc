# DLQ CDC Proof of Concept

A local, fully self-contained demo of a Dead Letter Queue (DLQ) pattern
protecting a CDC pipeline from data loss — no AWS account, no credentials,
no billing. Every AWS service is emulated locally via **LocalStack**.

## Architecture

```
MongoDB --> Debezium --> Redpanda (Kafka API) --> processor --+--> S3 (valid records)
                                                                 \-> SQS DLQ (malformed records)
                                                                        |
                                                                        v
                                                                  remediation.py
                                                                  (repair + replay, or
                                                                   leave visible for a human)
```

- **MongoDB** — source database
- **Debezium** — captures the MongoDB change stream (CDC)
- **Redpanda** — Kafka-API-compatible broker (stands in for MSK)
- **processor** — lightweight Python consumer; validates each record and
  routes it to S3 (valid) or the DLQ (malformed), never crashing or
  silently dropping a message
- **LocalStack** — emulates S3 and SQS
- **remediation.py** — reads the DLQ, logs the failure reason, and either
  auto-repairs and replays the record to S3, or leaves it on the DLQ for
  manual review if it can't be safely fixed

An optional real **Flink** job (`processor/flink_job.py`) using the same
validation core is included behind the `flink` Docker Compose profile, for
when you want to show the real Flink runtime rather than the lightweight
consumer.

## Prerequisites

- Docker + Docker Compose (Docker Engine via WSL2 on Windows, Docker
  Desktop, or native Docker on Mac/Linux)
- AWS CLI (`pip install awscli` or your OS package manager) — only used to
  talk to LocalStack, never a real AWS account

## Running it locally (recommended path)

```bash
./scripts/run_demo.sh
```

This brings up the full stack, creates the AWS resources, registers the
Debezium connector, seeds a valid and a poison-pill record, and prints the
processor's results plus a remediation run. Takes a few minutes on first
run while images are pulled (~6GB) and the processor image is built.

To tear down afterwards:
```bash
docker compose down -v
```

## Running it step by step

```bash
docker compose up --build -d
./scripts/setup_aws.sh      # S3 bucket + SQS main queue + DLQ (redrive policy)
./scripts/setup_kafka.sh    # Kafka topic + Debezium connector
docker restart dlq-processor
./scripts/seed_mongo.sh     # inserts 1 valid + 1 poison-pill record
docker logs -f dlq-processor
docker exec -i dlq-processor python remediation.py
```

## Running with the real Flink runtime

```bash
docker compose --profile flink up --build -d
```

## Sample data / demo story

- `data/valid_customer.json` — clean record, flows straight to S3
- `data/poison_customer.json` — **two** defects (`id` not numeric, missing
  `last_name`) — demonstrates the "caught by DLQ, visible, never silently
  lost" half of the story; it is not auto-repairable by design
- `data/recoverable_customer.json` — **one** benign defect (`id` arrives as
  a numeric string) — demonstrates the "auto-repaired and replayed to S3"
  half of the story

To see the full recovery path end to end, also run the processor against
the recoverable sample before remediation:
```bash
docker exec -i dlq-processor python consumer.py --input /app/../data/recoverable_customer.json
```
(or copy it into MongoDB via a small edit to `seed_mongo.sh`.)

## Running the CI workflow (GitHub Actions)

The workflow is manual-trigger only (`workflow_dispatch`) and never runs
automatically. After pushing this repo to GitHub:

Repo → **Actions** tab → **dlq-cdc-poc-e2e** → **Run workflow**

CI runs the unit tests, then a real Kafka→processor→S3/DLQ→remediation
integration test using LocalStack and Redpanda as free GitHub-hosted
service containers. It intentionally skips the MongoDB→Debezium leg (that
combination is flaky inside GitHub's service containers) and instead
exercises the processor directly against the sample JSON files — the full
Debezium capture path is exercised locally via `run_demo.sh`.

## No credentials anywhere

The only "credentials" used anywhere in this repo are LocalStack's dummy
placeholder values: `AWS_ACCESS_KEY_ID=test`, `AWS_SECRET_ACCESS_KEY=test`.
Nothing here touches a real AWS account.
