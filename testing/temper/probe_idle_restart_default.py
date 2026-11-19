"""Private probe: with the default source.offset, does a quiet restart lose data?

With IDLE=partial, step 2 writes K rows to partition 0 only, so partitions 1-3
stay idle while partition 0 moves.

No faults, and the source table sets no source.offset (the default, latest).
Each trial:
1. Writes N rows and waits until the pipeline copied them all.
2. Stops with a checkpoint and resumes, writing nothing, then waits through a
   few checkpoints. (A deploy in a quiet period.)
3. Stops with a checkpoint again. While it is stopped, writes M new rows.
4. Resumes and reads the output.

Expected: N + M rows. If the second resume restarts from `latest`, the M rows
written while it was stopped never arrive.

usage: probe_idle_restart_default.py MODE TRIALS [partial]
"""

import sys
import time
import uuid

from confluent_kafka import Producer

import harness

N, M = 2000, 500


def write(topic, start, count, partition=None):
    p = Producer({"bootstrap.servers": harness.KAFKA, "enable.idempotence": True})
    for i in range(start, start + count):
        kw = {} if partition is None else {"partition": partition}
        p.produce(topic, key=str(i).encode(), value=harness.json.dumps({"seq": i}).encode(), **kw)
    p.flush(60)


K = 100


def trial(mode, i, partial=False):
    name = f"idd-{mode[:2]}-{i}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = '{mode}');
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    time.sleep(5)  # let the source assign at `latest` before writing
    write(src, 0, N)
    _, reached = harness.read_committed(dst, "seq", N, timeout=60, quiet=4)
    assert reached, "first batch never arrived"
    harness.stop_and_resume(pid, "checkpoint")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    if partial:
        write(src, N + M, K, partition=0)  # only partition 0 moves
    time.sleep(8)  # several 2 s checkpoints with no input on the idle partitions
    harness.set_stop(pid, "checkpoint")
    assert harness.wait_state(pid, {"Stopped", "Failed"}) == "Stopped"
    write(src, N, M)  # written while the pipeline is stopped
    harness.set_stop(pid, "none")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    total = N + M + (K if partial else 0)
    values, _ = harness.read_committed(dst, "seq", total, timeout=60, quiet=10)
    missing, dupes = harness.summarize(values, total)
    harness.set_stop(pid, "immediate")
    span = f" ({missing[0]}..{missing[-1]})" if missing else ""
    print(f"{mode}{' partial' if partial else ''} trial {i}: rows={len(values)} (want {total}) missing={len(missing)}{span} "
          f"duplicated={len(dupes)}", flush=True)


if __name__ == "__main__":
    mode, trials = sys.argv[1], int(sys.argv[2])
    partial = len(sys.argv) > 3 and sys.argv[3] == "partial"
    harness.start_arroyo("controller")
    for i in range(trials):
        trial(mode, i, partial)
