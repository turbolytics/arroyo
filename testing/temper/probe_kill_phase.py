"""Private probe: does a worker kill outside the commit phase duplicate output?

Each trial copies 20,000 sequence numbers from one topic to another
(exactly-once, parallelism 2, 2-second checkpoints), kills the worker once at a
random moment, and counts duplicates a read_committed consumer sees. The kill
time is placed against the job's checkpoint log lines.

usage: probe_kill_phase.py TRIALS
"""

import datetime as dt
import re
import sys
import time
import uuid

import harness

N, RATE = 20000, 1000
LINE = re.compile(r"^(\S+Z).*?(Starting checkpointing|Finished checkpointing|Committing checkpoint|Finished committing checkpointing).*?job_id=\"(\w+)\" epoch=(\d+)")


def phases(job_id, since):
    out = harness.sh("docker", "logs", "--since", since, harness.CONTAINER).stdout + \
        harness.sh("docker", "logs", "--since", since, harness.CONTAINER).stderr
    events = []
    for raw in out.splitlines():
        line = re.sub(r"\x1b\[[0-9;]*m", "", raw)
        m = LINE.match(line)
        if m and m.group(3) == job_id:
            events.append((dt.datetime.fromisoformat(m.group(1).replace("Z", "+00:00")), m.group(2), int(m.group(4))))
    return events


def where(kill, events):
    before = [e for e in events if e[0] <= kill]
    if not before:
        return "before the first checkpoint"
    t, what, epoch = before[-1]
    ms = (kill - t).total_seconds() * 1000
    return f"{ms:.0f} ms after '{what}' epoch {epoch}"


def trial(i):
    name = f"phase-{i}-{uuid.uuid4().hex[:6]}"
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
    rng = harness.rng_for(name)
    time.sleep(rng.uniform(5, 14))
    kill_at = dt.datetime.now(dt.timezone.utc)
    msg = harness.kill_worker(rng)
    p.join()
    values, reached = harness.read_committed(dst, "seq", N, timeout=120)
    missing, dupes = harness.summarize(values, N)
    harness.set_stop(pid, "immediate")
    span = f"{dupes[0]}..{dupes[-1]}" if dupes else "-"
    print(f"trial {i}: {msg}; kill {where(kill_at, phases(job_id, since))}; "
          f"missing={len(missing)} dupes={len(dupes)} ({span})", flush=True)


if __name__ == "__main__":
    harness.start_arroyo("controller")
    for i in range(int(sys.argv[1])):
        trial(i)
