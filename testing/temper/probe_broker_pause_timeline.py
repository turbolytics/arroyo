"""Private probe: which restore writes the second copy after long broker pauses?

Runs the suite's schedule (three 75 s broker pauses, 5 to 8 s apart) on an
exactly-once pipeline, while a read_committed consumer reads the output the
whole time. Prints, every 5 s, rows and distinct rows seen so far, next to
Arroyo's state transitions and restores.

usage: probe_broker_pause_timeline.py
"""

import datetime as dt
import json
import re
import threading
import time
import uuid

from confluent_kafka import Consumer

import harness

N, RATE, PAUSE = 40000, 1000, 75
EVENT = re.compile(r'(state transition .*?from="(\w+)" to="(\w+)"|restoring checkpoint .*?epoch=(\d+)|'
                   r'panicked at [^ ]+|Producer creation failed|restored checkpoint was in committing phase)')


def main():
    harness.start_arroyo("controller")
    name = f"bpt-{uuid.uuid4().hex[:6]}"
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

    seen, rows = set(), [0]
    stop = threading.Event()

    def consume():
        c = Consumer({"bootstrap.servers": harness.KAFKA, "group.id": f"bpt-{uuid.uuid4().hex}",
                      "isolation.level": "read_committed", "auto.offset.reset": "earliest",
                      "enable.auto.commit": False})
        c.subscribe([dst])
        while not stop.is_set():
            m = c.poll(0.5)
            if m is not None and not m.error():
                rows[0] += 1
                seen.add(json.loads(m.value())["seq"])
        c.close()

    threading.Thread(target=consume, daemon=True).start()
    p = harness.SeqProducer(src, N, RATE)
    p.start()
    rng = harness.rng_for(name)

    def faults():
        for k in range(3):
            time.sleep(rng.uniform(5, 8))
            harness.pause_container(harness.KAFKA_CONTAINER, PAUSE)

    ft = threading.Thread(target=faults)
    ft.start()
    t0 = time.time()
    timeline = []
    while ft.is_alive() or time.time() - t0 < 3 * PAUSE + 180:
        timeline.append((dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S"), rows[0], len(seen)))
        if not ft.is_alive() and rows[0] >= N and len(timeline) > 3 and timeline[-4][1] == rows[0]:
            break
        time.sleep(5)
    stop.set()
    guard.check()

    logs = harness.sh("docker", "logs", "--since", since, harness.CONTAINER)
    text = harness.ANSI.sub("", logs.stdout + logs.stderr)
    events = []
    for l in text.splitlines():
        if job_id not in l and "Producer creation failed" not in l and "panicked" not in l:
            continue
        m = EVENT.search(l)
        if m:
            events.append((l[11:19], m.group(0)[:110]))
    merged = [(t, f"output rows={r} distinct={d}") for t, r, d in timeline] + events
    prev = None
    for t, what in sorted(merged):
        if what.startswith("output") and what == prev:
            continue
        print(t, what, flush=True)
        if what.startswith("output"):
            prev = what
    harness.set_stop(pid, "immediate")


if __name__ == "__main__":
    main()
