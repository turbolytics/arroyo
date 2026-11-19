# Temper map of Arroyo's delivery guarantees

This directory declares what Arroyo 0.16.0 promises, from its public docs, and
the faults a production pipeline should survive. `MATRIX.md` at the repository
root shows which promises hold. The tests in `testing/temper` are black-box:
they run the released Docker image and check its output.

## Paths

| Subject | Shape |
|---|---|
| `path.kafka_kafka_exactly_once` | Kafka to Kafka, `sink.commit_mode = 'exactly_once'` |
| `path.kafka_kafka_at_least_once` | Kafka to Kafka, the default commit mode |
| `path.kafka_files_json` | Kafka to raw JSON files on S3, 10 s roll |
| `path.kafka_files_parquet` | Kafka to raw Parquet files on S3, 10 s roll |
| `path.kafka_iceberg` | Kafka to an Iceberg table through a REST catalog, 60 s roll |

The three file paths follow Cloudflare Pipelines, which is built on Arroyo.
It writes raw JSON or Parquet to R2, or Iceberg through R2 Data Catalog, at
these minimum roll intervals. SeaweedFS stands in for R2.

Every path answers each member of `lattices/faults.contract.yml`: kills,
pauses, a partition, stalls past Kafka's transaction timeout, and two deploys
with no faults.

## Time scale

`testing/temper/test_paths.py` runs one scenario at a time with all five paths
side by side, so one fault hits every pipeline, as it would on a shared
cluster. Arroyo's clocks and the faults shrink together, so the same races
happen in seconds:

| Clock | Default | Here | Fault that crosses it |
|---|---|---|---|
| worker heartbeat timeout | 30 s | 12 s | worker pause, 16 s |
| Kafka transaction timeout | 60 s | 10 s | long stalls, 15 s |
| file roll | 10 s | 2 s | (commit cadence) |
| Iceberg roll | 60 s | 5 s | (commit cadence) |

At full scale, a 75 s stall wedges the exactly-once Kafka sink the same way a
15 s stall does here (`probe_txn_timeout_long.py` runs it at full scale).

## Setup

```sh
docker network create temper-arroyo

docker run -d --name temper-kafka --network temper-arroyo --network-alias kafka -p 29092:29092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:29092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:3.9.1

docker run -d --name temper-seaweed --network temper-arroyo --network-alias seaweed -p 28333:8333 \
  chrislusf/seaweedfs:3.97 server -dir=/data -s3 -s3.port=8333 -master.volumeSizeLimitMB=256 -volume.max=50
# Create the bucket, and an S3 identity (the Iceberg catalog signs its requests).
docker exec temper-seaweed sh -c 'echo "s3.bucket.create -name arroyo" | weed shell'
docker exec temper-seaweed sh -c 'echo "s3.configure -access_key=temper -secret_key=temper -user=temper -actions=Read,Write,List,Tagging,Admin -apply" | weed shell'

docker run -d --name temper-iceberg --network temper-arroyo --network-alias iceberg -p 28181:8181 \
  -e CATALOG_URI=jdbc:sqlite:file:/tmp/iceberg_catalog.db -e CATALOG_JDBC_USER=user -e CATALOG_JDBC_PASSWORD=password \
  -e CATALOG_WAREHOUSE=s3://arroyo/warehouse/ -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT=http://seaweed:8333 -e CATALOG_S3_PATH__STYLE__ACCESS=true \
  -e AWS_ACCESS_KEY_ID=temper -e AWS_SECRET_ACCESS_KEY=temper -e AWS_REGION=us-east-1 \
  apache/iceberg-rest-fixture:1.9.2

uv venv --python 3.12 .venv-temper
uv pip install --python .venv-temper/bin/python pytest temper-python confluent-kafka==2.16.1 \
  requests boto3 'pyiceberg[pyarrow]==0.10.0'
```

Each test session starts its own Arroyo container (`temper-arroyo`, port
25115) in the control-plane mode its level names.

## Run

```sh
temper clean
caffeinate -dims temper run --level controller -- .venv-temper/bin/pytest -q -s testing/temper
caffeinate -dims temper run --level worker_leader -- .venv-temper/bin/pytest -q -s testing/temper
temper matrix --all-levels
```

A level takes about 10 minutes. A host that sleeps pauses Docker, so the tests
check the wall clock against the monotonic clock and void the run if they
drift.

The `probe_*.py` scripts are the focused repros behind the findings.
