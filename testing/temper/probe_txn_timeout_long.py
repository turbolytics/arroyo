"""Private probe: after a stall past the transaction timeout, is the loss permanent?

One exactly-once pipeline per trial. Storage pauses PAUSE seconds. Then:
1. Watch the output count and checkpoint lines for OBSERVE seconds.
2. Stop the pipeline with a checkpoint, resume it, and read again.
If rows are still missing after the restart, they are lost, not late.

usage: probe_txn_timeout_long.py PAUSE TRIALS OBSERVE [CLIENT_CONFIGS]
"""

import re
import sys
import time
import uuid

import harness

N, RATE = 40000, 1000


def count(dst):
    values, _ = harness.read_committed(dst, "seq", N, timeout=20, quiet=2)
    return len(set(values))


EXTRA = ""


def trial(i, pause, observe):
    name = f"ttl-{pause}-{i}-{uuid.uuid4().hex[:6]}"
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = 'exactly_once'{EXTRA});
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    job_id = harness.job(pid)["id"]
    guard = harness.SleepGuard()
    watcher = harness.LogWatcher(job_id)
    p = harness.SeqProducer(src, N, RATE)
    p.start()
    time.sleep(8)
    t_pause = time.time()
    harness.pause_storage(pause)
    p.join()
    samples = []
    end = time.time() + observe
    while time.time() < end:
        samples.append((round(time.time() - t_pause), count(dst), harness.job(pid).get("state")))
        time.sleep(20)
    after = [e for e in watcher.events if e[0] > t_pause + pause and e[1] == "Finished committing checkpointing"]
    watcher.stop()
    print(f"trial {i} pause {pause}s: samples (s after pause, distinct rows, state) {samples}", flush=True)
    print(f"  commits finished after the pause: {len(after)}", flush=True)
    try:
        harness.stop_and_resume(pid, "checkpoint")
        how = "stop checkpoint, resumed"
    except harness.FaultNotInjected as e:
        harness.set_stop(pid, "immediate")
        harness.wait_state(pid, {"Stopped", "Failed"}, timeout=120)
        harness.set_stop(pid, "none")
        how = f"checkpoint stop failed ({e}); stop immediate, resumed"
    harness.wait_state(pid, {"Running"}, timeout=120)
    values, reached = harness.read_committed(dst, "seq", N, timeout=180)
    missing, dupes = harness.summarize(values, N)
    harness.set_stop(pid, "immediate")
    guard.check()
    span = f"{missing[0]}..{missing[-1]}" if missing else "-"
    print(f"  after restart ({how}): rows={len(values)} missing={len(missing)} ({span}) dupes={len(dupes)} "
          f"reached={reached}", flush=True)


if __name__ == "__main__":
    pause, trials, observe = map(int, sys.argv[1:4])
    if len(sys.argv) > 4:
        EXTRA = f", client_configs = '{sys.argv[4]}'"
    harness.start_arroyo("controller")
    for i in range(trials):
        trial(i, pause, observe)
