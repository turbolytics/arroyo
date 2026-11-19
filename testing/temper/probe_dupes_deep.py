"""Private probe: where do the duplicates come from?

Runs single-kill trials until one duplicates output, then reports:
- whether the input topic has duplicates (it must not),
- the partition and offset of both copies of a few duplicated messages,
- Arroyo's log lines about restoring, checkpoints and commits around the kill.
"""

import datetime as dt
import json
import re
import time
import uuid
from collections import defaultdict

from confluent_kafka import Consumer

import harness

N, RATE = 20000, 1000


def dump(topic, isolation):
    c = Consumer({"bootstrap.servers": harness.KAFKA, "group.id": f"deep-{uuid.uuid4().hex}",
                  "isolation.level": isolation, "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    out, idle = [], time.time()
    while time.time() - idle < 8:
        m = c.poll(0.5)
        if m is not None and not m.error():
            out.append((json.loads(m.value())["seq"], m.partition(), m.offset()))
            idle = time.time()
    c.close()
    return out


def trial(i):
    name = f"deep-{i}-{uuid.uuid4().hex[:6]}"
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    since = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}');
INSERT INTO dst SELECT seq FROM src;""")
    harness.wait_state(pid, {"Running"})
    job_id = harness.job(pid)["id"]
    p = harness.SeqProducer(src, N, RATE)
    p.start()
    time.sleep(8.8)
    kill_at = dt.datetime.now(dt.timezone.utc)
    harness.kill_worker(harness.rng_for(name))
    p.join()
    harness.read_committed(dst, "seq", N, timeout=120)  # wait for catch-up
    out = dump(dst, "read_committed")
    copies = defaultdict(list)
    for seq, part, off in out:
        copies[seq].append((part, off))
    dupes = sorted(s for s, c in copies.items() if len(c) > 1)
    print(f"trial {i}: kill at {kill_at.isoformat()}, output rows={len(out)}, dupes={len(dupes)}", flush=True)
    if not dupes:
        harness.set_stop(pid, "immediate")
        return False
    inp = dump(src, "read_committed")
    in_seqs = [s for s, _, _ in inp]
    print(f"  input rows={len(inp)} distinct={len(set(in_seqs))} (duplicates in input: {len(in_seqs) - len(set(in_seqs))})")
    print(f"  duplicated seq range {dupes[0]}..{dupes[-1]}")
    for s in [dupes[0], dupes[len(dupes) // 2], dupes[-1]]:
        print(f"  seq {s}: copies at (partition, offset) {copies[s]}")
    per_part = defaultdict(list)
    for s in dupes:
        for part, off in copies[s]:
            per_part[part].append(off)
    for part, offs in sorted(per_part.items()):
        offs.sort()
        print(f"  partition {part}: duplicate copies span offsets {offs[0]}..{offs[-1]} ({len(offs)} records)")
    logs = harness.sh("docker", "logs", "--since", since, harness.CONTAINER)
    text = re.sub(r"\x1b\[[0-9;]*m", "", logs.stdout + logs.stderr)
    keep = re.compile(r"restor|Restor|checkpoint|Checkpoint|commit|Commit|transaction|failed|Failed|epoch|Recover|recover")
    lines = [l for l in text.splitlines() if job_id in l or "worker" in l.lower()]
    lines = [l for l in lines if keep.search(l)]
    window = [l for l in lines if l[:19] >= (kill_at - dt.timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")]
    print("  log lines from 5s before the kill:")
    for l in window[:45]:
        print("   ", l[:230])
    harness.set_stop(pid, "immediate")
    return True


if __name__ == "__main__":
    harness.start_arroyo("controller")
    for i in range(5):
        if trial(i):
            break
