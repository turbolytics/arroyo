"""Delivery invariants and faults.contract for the Kafka-to-Kafka paths.

Each (path, fault) pair gets one run. A producer writes sequence numbers
0..N-1 to a 4-partition topic while the pipeline copies them to another topic
(parallelism 2, a checkpoint every 2 seconds). The fault fires three times.
Afterwards a read_committed consumer reads the output topic.

The expected output is the input: every sequence number, once for exactly-once
and at least once for at-least-once.

The code defaults to at_least_once, so each sink states its commit mode. Kills
are timed outside the commit phase, which the docs exempt (#224), and a kill
that lands inside it errors the test instead of counting.
"""

import time
import uuid

import pytest

import harness

N = 40000
RATE = 1000  # 40 seconds of input
RECOVERY_SECONDS = 120
COMMON = ["worker_kill", "node_kill", "stop_with_checkpoint", "stop_immediately", "storage_outage"]
PATHS = {
    # subject: (commit mode, faults it answers supported)
    "path.kafka_kafka_exactly_once": ("exactly_once", COMMON),
    "path.kafka_kafka_at_least_once": ("at_least_once", COMMON + ["kill_during_commit"]),
}
FEATURES = {"path.kafka_kafka_exactly_once": "sink.kafka.exactly_once",
            "path.kafka_kafka_at_least_once": "sink.kafka.at_least_once"}


def run(subject, fault):
    mode = PATHS[subject][0]
    name = f"{mode[:2]}-{fault}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (
    connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest'
);
CREATE TABLE dst (seq BIGINT) WITH (
    connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}',
    'sink.commit_mode' = '{mode}'
);
INSERT INTO dst SELECT seq FROM src;
""")
    assert harness.wait_state(pid, {"Running"}) == "Running", "pipeline never started"
    guard = harness.SleepGuard()
    producer = harness.SeqProducer(src, N, RATE)
    producer.start()
    rng = harness.rng_for(name)
    watcher = harness.LogWatcher(harness.job(pid)["id"])
    log = []
    try:
        for _ in range(3):
            time.sleep(rng.uniform(5, 8))
            log.append(harness.inject(fault, pid, rng, watcher))
            harness.wait_state(pid, {"Running"}, timeout=60)
    finally:
        watcher.stop()
    producer.join()
    values, reached = harness.read_committed(dst, "seq", N, timeout=RECOVERY_SECONDS)
    state = harness.job(pid).get("state")
    missing, dupes = harness.summarize(values, N)
    harness.set_stop(pid, "immediate")
    guard.check()
    return {"log": log, "reached": reached, "state": state, "missing": missing, "dupes": dupes,
            "rows": len(values)}


_cache = {}


def result(subject, fault):
    key = (subject, fault)
    if key not in _cache:
        try:
            _cache[key] = run(subject, fault)
        except harness.HostSlept as e:
            # Sleep pauses Docker and expires Kafka sessions, so every later
            # result is suspect too. Void the whole run instead of failing tests.
            pytest.exit(f"run void: {e}", returncode=3)
        r = _cache[key]
        print(f"\n[{subject} {fault}] {r['log']} state={r['state']} rows={r['rows']} "
              f"missing={len(r['missing'])} dupes={len(r['dupes'])} reached={r['reached']}")
    return _cache[key]


def cases(ref_for, subjects=PATHS):
    out = []
    for subject in subjects:
        for fault in PATHS[subject][1]:
            ref = ref_for(fault)
            marks = [pytest.mark.covers(ref, subject=subject)] if ref else []
            out.append(pytest.param(subject, fault, id=f"{subject.split('.')[1]}-{fault}", marks=marks))
    return out


def lost(r):
    return f"{len(r['missing'])} of {N} messages missing, e.g. {r['missing'][:10]}; faults: {r['log']}"


@pytest.mark.parametrize("subject,fault", [
    pytest.param(s, f, id=f"{s.split('.')[1]}-{f}", marks=[
        pytest.mark.covers(f"faults.contract/{f}", subject=s), pytest.mark.covers("source.kafka"),
        pytest.mark.covers(FEATURES[s])])
    for s in PATHS for f in PATHS[s][1]])
def test_fault_keeps_every_delivery_invariant(subject, fault):
    r = result(subject, fault)
    assert not r["missing"], lost(r)
    assert r["reached"] and r["state"] == "Running", f"no recovery; state {r['state']}"
    if PATHS[subject][0] == "exactly_once":
        assert not r["dupes"], f"{len(r['dupes'])} duplicated, e.g. {r['dupes'][:10]}"


@pytest.mark.parametrize("subject,fault", cases(lambda f: "delivery.no_loss"))
def test_no_loss(subject, fault):
    r = result(subject, fault)
    assert not r["missing"], lost(r)


@pytest.mark.parametrize("subject,fault", cases(lambda f: "delivery.no_duplicates",
                                                ["path.kafka_kafka_exactly_once"]))
def test_no_duplicates(subject, fault):
    r = result(subject, fault)
    assert not r["dupes"], f"{len(r['dupes'])} messages delivered more than once, e.g. {r['dupes'][:10]}; faults: {r['log']}"


@pytest.mark.parametrize("subject,fault", cases(lambda f: "delivery.recovers"))
def test_recovers(subject, fault):
    r = result(subject, fault)
    assert r["reached"] and r["state"] == "Running", \
        f"output did not catch up within {RECOVERY_SECONDS}s; job state {r['state']}; faults: {r['log']}"


@pytest.mark.exempts("delivery.no_duplicates", subject="path.kafka_kafka_at_least_once")
def test_at_least_once_loses_nothing():
    """At-least-once may repeat rows after a failure, as its docs say, but never loses one."""
    r = result("path.kafka_kafka_at_least_once", "worker_kill")
    assert not r["missing"], lost(r)
