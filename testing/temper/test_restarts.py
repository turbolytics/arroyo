"""Delivery invariants across deploys, with no faults.

A deploy is a stop with a checkpoint followed by a resume. Each case uses the
Kafka source's default `source.offset` (latest).

quiet_restart:
1. Write N rows and wait until the pipeline copies them.
2. Deploy, with no input. Wait through several 2 s checkpoints.
3. Stop with a checkpoint. Write M rows while the pipeline is stopped.
4. Resume. The output must hold all N + M rows.

idle_partition_restart: the same, but in step 2, K rows go to partition 0 only.
Partitions 1 to 3 stay idle.
"""

import time
import uuid

import pytest
from confluent_kafka import Producer

import harness

N, M, K = 2000, 500, 100
SCENARIOS = ["quiet_restart", "idle_partition_restart"]
MODES = {"path.kafka_kafka_exactly_once": "exactly_once", "path.kafka_kafka_at_least_once": "at_least_once"}
FEATURES = {"path.kafka_kafka_exactly_once": "sink.kafka.exactly_once",
            "path.kafka_kafka_at_least_once": "sink.kafka.at_least_once"}


def write(topic, start, count, partition=None):
    p = Producer({"bootstrap.servers": harness.KAFKA, "enable.idempotence": True})
    kw = {} if partition is None else {"partition": partition}
    for i in range(start, start + count):
        p.produce(topic, key=str(i).encode(), value=harness.json.dumps({"seq": i}).encode(), **kw)
    p.flush(60)


def run(subject, scenario):
    mode = MODES[subject]
    name = f"rs-{mode[:2]}-{scenario[:4]}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = '{mode}');
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running", "pipeline never started"
    guard = harness.SleepGuard()
    time.sleep(5)  # the source assigns at `latest` first
    write(src, 0, N)
    _, reached = harness.read_committed(dst, "seq", N, timeout=60, quiet=4)
    assert reached, "the first rows never arrived"
    harness.stop_and_resume(pid, "checkpoint")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    total = N + M
    if scenario == "idle_partition_restart":
        write(src, N + M, K, partition=0)
        total += K
    time.sleep(8)
    harness.set_stop(pid, "checkpoint")
    if harness.wait_state(pid, {"Stopped", "Failed"}) != "Stopped":
        raise harness.FaultNotInjected("the second stop did not reach Stopped")
    write(src, N, M)
    harness.set_stop(pid, "none")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    values, reached = harness.read_committed(dst, "seq", total, timeout=60, quiet=10)
    missing, dupes = harness.summarize(values, total)
    harness.set_stop(pid, "immediate")
    guard.check()
    return {"total": total, "rows": len(values), "missing": missing, "dupes": dupes}


_cache = {}


def result(subject, scenario):
    key = (subject, scenario)
    if key not in _cache:
        try:
            _cache[key] = run(subject, scenario)
        except harness.HostSlept as e:
            pytest.exit(f"run void: {e}", returncode=3)
        r = _cache[key]
        print(f"\n[{subject} {scenario}] rows={r['rows']} want={r['total']} "
              f"missing={len(r['missing'])} dupes={len(r['dupes'])}")
    return _cache[key]


def params(invariant=None, subjects=MODES):
    out = []
    for s in subjects:
        for sc in SCENARIOS:
            marks = [pytest.mark.covers(invariant, subject=s)] if invariant else [
                pytest.mark.covers(f"faults.contract/{sc}", subject=s),
                pytest.mark.covers("source.kafka"), pytest.mark.covers(FEATURES[s])]
            out.append(pytest.param(s, sc, id=f"{s.split('.')[1]}-{sc}", marks=marks))
    return out


def lost(r):
    return f"{len(r['missing'])} of {r['total']} rows missing, e.g. {r['missing'][:10]}"


@pytest.mark.parametrize("subject,scenario", params())
def test_deploys_keep_every_delivery_invariant(subject, scenario):
    r = result(subject, scenario)
    assert not r["missing"], lost(r)
    if MODES[subject] == "exactly_once":
        assert not r["dupes"], f"{len(r['dupes'])} rows delivered twice, e.g. {r['dupes'][:10]}"


@pytest.mark.parametrize("subject,scenario", params("delivery.no_loss"))
def test_deploys_lose_nothing(subject, scenario):
    r = result(subject, scenario)
    assert not r["missing"], lost(r)


@pytest.mark.parametrize("subject,scenario", params("delivery.no_duplicates", ["path.kafka_kafka_exactly_once"]))
def test_deploys_duplicate_nothing(subject, scenario):
    r = result(subject, scenario)
    assert not r["dupes"], f"{len(r['dupes'])} rows delivered twice, e.g. {r['dupes'][:10]}"
