"""Private probe: does a stall longer than Kafka's transaction timeout lose data?

Arroyo's exactly-once Kafka sink keeps one transaction open per checkpoint and
does not set transaction.timeout.ms, so librdkafka's 60 s default applies. A
storage outage stalls the checkpoint while the transaction stays open. Past
60 s the broker aborts the transaction on its own.

Each trial copies N sequence numbers (exactly-once, parallelism 2, 2 s
checkpoints), pauses SeaweedFS once for PAUSE seconds, and counts what a
read_committed consumer sees. A 30 s pause is the control.

usage: probe_txn_timeout.py PAUSE [PAUSE ...]
"""

import datetime as dt
import re
import sys
import time
import uuid

import harness

N, RATE = 40000, 1000
KEEP = re.compile(r"commit|Commit|abort|Abort|fenc|Fenc|timed out|timeout|panic|Panic|restor|Restor|failed|Failed|Error|error")


def trial(pause):
    name = f"tt-{pause}-{uuid.uuid4().hex[:6]}"
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    since = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = 'exactly_once');
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    job_id = harness.job(pid)["id"]
    guard = harness.SleepGuard()
    p = harness.SeqProducer(src, N, RATE)
    p.start()
    time.sleep(8)
    harness.pause_storage(pause)
    p.join()
    values, reached = harness.read_committed(dst, "seq", N, timeout=240)
    state = harness.job(pid).get("state")
    missing, dupes = harness.summarize(values, N)
    harness.set_stop(pid, "immediate")
    guard.check()
    span = f"{missing[0]}..{missing[-1]}" if missing else "-"
    print(f"pause {pause}s: state={state} rows={len(values)} missing={len(missing)} ({span}) "
          f"dupes={len(dupes)} reached={reached}", flush=True)
    logs = harness.sh("docker", "logs", "--since", since, harness.CONTAINER)
    text = harness.ANSI.sub("", logs.stdout + logs.stderr)
    lines = [l for l in text.splitlines() if KEEP.search(l) and "Starting checkpointing" not in l]
    seen = set()
    for l in lines:
        key = re.sub(r"^\S+\s+", "", l)[:120]
        if key not in seen:
            seen.add(key)
            print("   ", l[:240])
        if len(seen) >= 30:
            break


if __name__ == "__main__":
    harness.start_arroyo("controller")
    for pause in sys.argv[1:]:
        trial(int(pause))
