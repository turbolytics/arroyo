"""File and Iceberg sinks on object storage, shaped like Cloudflare Pipelines.

Cloudflare Pipelines (built on Arroyo) writes raw JSON or Parquet files to R2,
or Iceberg tables through R2 Data Catalog, with a time-based roll (300 s by
default, at least 10 s for raw files and 60 s for Iceberg). Here SeaweedFS
stands in for R2 and Apache's Iceberg REST fixture for the catalog.

Each sink has a DDL fragment and a reader that returns every committed row.
"""

import io
import json
import uuid

import boto3
import pyarrow.parquet as pq

import harness

S3_ENDPOINT = "http://localhost:28333"
BUCKET = "arroyo"
CATALOG_URL = "http://localhost:28181"
CATALOG_INSIDE = "http://iceberg:8181"
ICEBERG_CONTAINER = "temper-iceberg"

S3 = boto3.client("s3", endpoint_url=S3_ENDPOINT, aws_access_key_id="temper",
                  aws_secret_access_key="temper", region_name="us-east-1")

# Cloudflare's minimum roll intervals.
ROLL = {"json": 10, "parquet": 10, "iceberg": 60}


def ddl(kind, run, roll=None):
    """The WITH options for a sink table named `dst`, and how to find its output."""
    roll = roll or ROLL[kind]
    if kind in ("json", "parquet"):
        return (f"connector = 'filesystem', type = 'sink', format = '{kind}', "
                f"path = 's3://{BUCKET}/files/{run}/', "
                f"'rolling_policy.interval' = interval '{roll} seconds'")
    if kind == "iceberg":
        return (f"connector = 'iceberg', type = 'sink', format = 'parquet', "
                f"'catalog.type' = 'rest', 'catalog.rest.url' = '{CATALOG_INSIDE}', "
                f"namespace = 'temper', table_name = '{table(run)}', "
                # The Iceberg writer takes its S3 settings from these, not AWS_ENDPOINT.
                f"'storage.aws_endpoint' = 'http://seaweed:8333', 'storage.aws_region' = 'us-east-1', "
                f"'storage.aws_access_key_id' = 'temper', 'storage.aws_secret_access_key' = 'temper', "
                f"'rolling_policy.interval' = interval '{roll} seconds'")
    raise ValueError(kind)


def table(run):
    return run.replace("-", "_")


def files(run):
    keys, token = [], None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": f"files/{run}/"}
        if token:
            kw["ContinuationToken"] = token
        page = S3.list_objects_v2(**kw)
        keys += [o["Key"] for o in page.get("Contents", [])]
        if not page.get("IsTruncated"):
            return keys
        token = page["NextContinuationToken"]


class Truncated(AssertionError):
    """A committed file that does not parse completely."""


def read(kind, run, field="seq"):
    """Every committed row's `field`, in no particular order."""
    if kind == "json":
        out = []
        for key in files(run):
            body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
            for n, line in enumerate(body.splitlines()):
                if not line.strip():
                    continue
                try:
                    out.append(json.loads(line)[field])
                except json.JSONDecodeError as e:
                    raise Truncated(f"{key} line {n + 1} does not parse: {e}") from e
        return out
    if kind == "parquet":
        out = []
        for key in files(run):
            body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
            try:
                out += pq.read_table(io.BytesIO(body)).column(field).to_pylist()
            except Exception as e:  # pyarrow raises several types for a bad file
                raise Truncated(f"{key} does not parse as Parquet: {e}") from e
        return out
    if kind == "iceberg":
        from pyiceberg.catalog import load_catalog
        from pyiceberg.exceptions import NoSuchTableError
        cat = load_catalog("temper", type="rest", uri=CATALOG_URL, **{
            "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": "temper",
            "s3.secret-access-key": "temper", "s3.region": "us-east-1", "s3.path-style-access": "true"})
        try:
            t = cat.load_table(("temper", table(run)))
        except NoSuchTableError:
            return []
        if t.current_snapshot() is None:
            return []
        return t.scan(selected_fields=(field,)).to_arrow().column(field).to_pylist()
    raise ValueError(kind)


def create_table(run):
    """Creates the Iceberg table a pipeline will write, before it starts.

    With parallelism above 1, sink subtasks race to create a new table, and one
    fails with "table already exists". The controller recovers in a second; the
    worker leader waits out a 120 s stop timeout first. The fault tests are about
    recovery, not first-time setup, so they start with the table in place.
    """
    from pyiceberg.catalog import load_catalog
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField
    cat = load_catalog("temper", type="rest", uri=CATALOG_URL, **{
        "s3.endpoint": S3_ENDPOINT, "s3.access-key-id": "temper",
        "s3.secret-access-key": "temper", "s3.region": "us-east-1", "s3.path-style-access": "true"})
    cat.create_table(("temper", table(run)), Schema(NestedField(1, "seq", LongType(), required=False)))


def ensure_namespace():
    from pyiceberg.catalog import load_catalog
    cat = load_catalog("temper", type="rest", uri=CATALOG_URL)
    if ("temper",) not in cat.list_namespaces():
        cat.create_namespace("temper")


def wait_rows(kind, run, n, timeout, quiet=None):
    """Polls the committed output until it holds n distinct values, then waits
    one more roll interval so late duplicates show up too. Returns (values, reached)."""
    import time
    quiet = quiet if quiet is not None else ROLL[kind] + 10
    deadline = time.time() + timeout
    reached_at, values = None, []
    while time.time() < deadline:
        values = read(kind, run)
        if reached_at is None and len(set(values)) >= n:
            reached_at = time.time()
        if reached_at is not None and time.time() - reached_at >= quiet:
            return read(kind, run), True
        time.sleep(3)
    return values, reached_at is not None


def new_run(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"
