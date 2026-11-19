"""Private probe: why does a 75 s broker pause duplicate every row under exactly-once?

One exactly-once pipeline. The broker pauses once for PAUSE seconds. Then:
- Is the input topic itself duplicated? (It must not be.)
- Where do both copies of a few duplicated rows sit (partition, offset)?
- Which checkpoint did Arroyo restore, and what did it log about commits?

usage: probe_broker_pause.py PAUSE [COUNT [CLIENT_CONFIGS]]
"""

import datetime as dt
import re
import sys
import time
import uuid
from collections import defaultdict

from confluent_kafka import Consumer

import harness

N, RATE = 40000, 1000
KEEP = re.compile(r"restor|Restor|commit|Commit|fenc|Fenc|abort|Abort|panic|failed|Failed|epoch=0|TXNERR|FATAL|timed out|Scheduling|scheduling|task failed")


def dump(topic):
    c = Consumer({"bootstrap.servers": harness.KAFKA, "group.id": f"bp-{uuid.uuid4().hex}",
                  "isolation.level": "read_committed", "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, idle = [], time.time()
    while time.time() - idle < 10:
        m = c.poll(0.5)
        if m is not None and not m.error():
            out.append((harness.json.loads(m.value())["seq"], m.partition(), m.offset()))
            idle = time.time()
    c.close()
    return out


def main(pause, count=1, extra=""):
    harness.start_arroyo("controller")
    name = f"bp-{pause}-{uuid.uuid4().hex[:6]}"
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    since = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = 'exactly_once'{extra});
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    job_id = harness.job(pid)["id"]
    guard = harness.SleepGuard()
    p = harness.SeqProducer(src, N, RATE)
    p.start()
    rng = harness.rng_for(name)
    t0 = dt.datetime.now(dt.timezone.utc)
    for k in range(count):
        time.sleep(rng.uniform(5, 8))  # the suite's schedule
        print(f"pause {k + 1} at {dt.datetime.now(dt.timezone.utc).isoformat()}", flush=True)
        harness.pause_container(harness.KAFKA_CONTAINER, pause)
        print(f"  job state after unpause: {harness.wait_state(pid, {'Running'}, timeout=60)}", flush=True)
    p.join()
    harness.read_committed(dst, "seq", N, timeout=180)
    out = dump(dst)
    inp = dump(src)
    guard.check()
    in_seqs = [s for s, _, _ in inp]
    print(f"pause at {t0.isoformat()} for {pause}s")
    print(f"input: rows={len(inp)} distinct={len(set(in_seqs))}")
    copies = defaultdict(list)
    for s, part, off in out:
        copies[s].append((part, off))
    dupes = sorted(s for s, c in copies.items() if len(c) > 1)
    print(f"output: rows={len(out)} distinct={len(copies)} duplicated={len(dupes)}")
    if dupes:
        for s in [dupes[0], dupes[len(dupes) // 2], dupes[-1]]:
            print(f"  seq {s}: copies at (partition, offset) {copies[s]}")
        per_part = defaultdict(list)
        for s, part, off in out:
            per_part[part].append((off, s))
        for part, rows in sorted(per_part.items()):
            rows.sort()
            seqs = [s for _, s in rows]
            first_repeat = next((i for i, s in enumerate(seqs) if s in set(seqs[:i])), None)
            print(f"  partition {part}: {len(rows)} rows, offsets {rows[0][0]}..{rows[-1][0]}, "
                  f"first repeated row at position {first_repeat}")
    logs = harness.sh("docker", "logs", "--since", since, harness.CONTAINER)
    text = harness.ANSI.sub("", logs.stdout + logs.stderr)
    seen = set()
    print("log lines (deduplicated):")
    for l in text.splitlines():
        if not KEEP.search(l) or "Starting checkpointing" in l:
            continue
        if re.search(r"(Committing checkpoint|Finishing committing|Finished committing checkpointing)", l):
            continue
        key = re.sub(r"^\S+\s+", "", l)
        key = re.sub(r"\d+", "#", key)[:140]
        if key in seen:
            continue
        seen.add(key)
        print("   ", l[:260])
    harness.set_stop(pid, "immediate")


if __name__ == "__main__":
    extra = f", client_configs = '{sys.argv[3]}'" if len(sys.argv) > 3 else ""
    main(int(sys.argv[1]), int(sys.argv[2]) if len(sys.argv) > 2 else 1, extra)
