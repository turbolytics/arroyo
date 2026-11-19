"""Private probe: does an Iceberg sink start in worker-leader mode?

Default Arroyo settings (no time scaling). One Kafka-to-Iceberg pipeline into a
new table, with no faults. Records how long the job takes to reach Running and
to commit all N rows, for each mode and parallelism.

usage: probe_iceberg_leader.py MODE PARALLELISM [MODE PARALLELISM ...]
       MODE is controller or worker
"""

import sys
import time

import files
import harness

N = 5000


def trial(parallelism):
    run = files.new_run(f"il-p{parallelism}")
    src = f"{run}-in"
    harness.create_topics(src)
    t0 = time.time()
    pid = harness.create_pipeline(run, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH ({files.ddl('iceberg', run, 10)});
INSERT INTO dst SELECT seq FROM src;""", parallelism=parallelism)
    states, state, running_at = [], None, None
    while time.time() - t0 < 300:
        s = harness.job(pid).get("state")
        if s != state:
            states.append(f"{time.time() - t0:.0f}s {s}")
            state = s
        if s == "Running" and running_at is None:
            running_at = time.time() - t0
            break
        time.sleep(1)
    rows = None
    if running_at is not None:
        p = harness.SeqProducer(src, N, 2000)
        p.start()
        p.join()
        values, reached = files.wait_rows("iceberg", run, N, timeout=120, quiet=0)
        rows = f"{len(set(values))}/{N} committed" if reached else f"only {len(set(values))}/{N} after 120 s"
    harness.set_stop(pid, "immediate")
    print(f"  parallelism {parallelism}: running after {running_at if running_at is None else round(running_at)} s; "
          f"rows {rows}; states {states}", flush=True)


if __name__ == "__main__":
    args = sys.argv[1:]
    for mode, par in zip(args[::2], args[1::2]):
        harness.start_arroyo(mode)  # default heartbeat timeout
        files.ensure_namespace()
        print(f"mode {mode}:", flush=True)
        trial(int(par))
