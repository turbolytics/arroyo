"""The sink.contract lattice (temper/lattices/sink.contract.yml).

Each test states what it proves with @pytest.mark.covers (temper-python).
Without `temper run`, the marks do nothing.

A counter source emits 0..N-1 and then finishes, as a FileSystem source does
after its last file. The job takes checkpoints every 10 seconds (the default).
After it reports Finished, the tests read every committed file.

The first test uses the sink's default options. The second rolls files every
3 seconds, so that files commit while the job runs.
"""

import json
import os
import subprocess
import time
import uuid

import boto3
import pytest
import requests

API = os.environ.get("TEMPER_ARROYO_API", "http://localhost:25115/api/v1")
CONTAINER = os.environ.get("TEMPER_ARROYO_CONTAINER", "temper-arroyo")
S3 = boto3.client("s3", endpoint_url=os.environ.get("TEMPER_S3", "http://localhost:28333"),
                  aws_access_key_id="temper", aws_secret_access_key="temper", region_name="us-east-1")
BUCKET = "arroyo"
RATE = 2000
SHORT = 20000  # 10 seconds at 2,000 rows a second
LONG = 200000  # long enough for several checkpoints

SINKS = {
    "v2": "path = 's3://arroyo/temper/{run}/'",
    "v1": "path = 's3://arroyo/temper/{run}/', 'sink.version' = 'v1'",
    "local": "path = '/tmp/temper/{run}'",
}


def sinks(member):
    return [
        pytest.param(name, id=name, marks=[pytest.mark.covers(f"sink.contract/{member}", subject=f"sink.{name}"),
                                           pytest.mark.covers(f"sink.{name}")])
        for name in SINKS
    ]


def run_job(sink, count, options):
    run = f"{sink}-{uuid.uuid4().hex[:8]}"
    query = f"""
CREATE TABLE impulse WITH (connector = 'impulse', event_rate = '{RATE}', message_count = '{count}');
CREATE TABLE out (counter BIGINT) WITH (
    connector = 'filesystem', type = 'sink', format = 'json',
    {SINKS[sink].format(run=run)}{options}
);
INSERT INTO out SELECT cast(counter as bigint) FROM impulse;
"""
    r = requests.post(f"{API}/pipelines", json={"name": run, "query": query, "parallelism": 1})
    r.raise_for_status()
    pid = r.json()["id"]
    deadline = time.time() + count / RATE + 900
    state = None
    while time.time() < deadline:
        jobs = requests.get(f"{API}/pipelines/{pid}/jobs").json()["data"]
        state = jobs[0]["state"] if jobs else None
        if state in ("Finished", "Failed", "Stopped"):
            break
        time.sleep(1)
    time.sleep(2)
    return state, rows(sink, run)


def rows(sink, run):
    out = []
    if sink == "local":
        text = subprocess.run(["docker", "exec", CONTAINER, "sh", "-c",
                               f"find /tmp/temper/{run} -maxdepth 1 -type f -exec cat {{}} +"],
                              capture_output=True, text=True).stdout
        return [json.loads(line)["counter"] for line in text.splitlines() if line.strip()]
    for obj in S3.list_objects_v2(Bucket=BUCKET, Prefix=f"temper/{run}/").get("Contents", []):
        body = S3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode()
        out += [json.loads(line)["counter"] for line in body.splitlines() if line.strip()]
    return out


@pytest.mark.parametrize("sink", sinks("every_row_written_when_the_source_finishes"))
def test_every_row_written_when_the_source_finishes(sink):
    state, got = run_job(sink, SHORT, "")
    assert state == "Finished"
    missing = SHORT - len(set(got))
    assert missing == 0, f"the job reported Finished, but {missing} of {SHORT} rows are missing"


@pytest.mark.parametrize("sink", sinks("committed_rows_have_no_gaps_or_duplicates"))
def test_committed_rows_have_no_gaps_or_duplicates(sink):
    state, got = run_job(sink, LONG, ", 'rolling_policy.interval' = interval '3 seconds'")
    assert state == "Finished"
    assert got, "nothing was committed"
    assert sorted(got) == list(range(len(got))), "committed rows have a gap or a duplicate"
