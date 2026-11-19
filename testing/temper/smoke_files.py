"""Smoke test: Kafka to each file sink, no faults. usage: smoke_files.py [KIND ...]"""

import sys
import time

import files
import harness

N = 5000


def smoke(kind):
    run = files.new_run(f"smoke-{kind}")
    src = f"{run}-in"
    harness.create_topics(src)
    pid = harness.create_pipeline(run, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH ({files.ddl(kind, run)});
INSERT INTO dst SELECT seq FROM src;""")
    state = harness.wait_state(pid, {"Running", "Failed"})
    print(kind, "state", state, flush=True)
    p = harness.SeqProducer(src, N, 2000)
    p.start()
    p.join()
    t0 = time.time()
    values, reached = files.wait_rows(kind, run, N, timeout=files.ROLL[kind] * 3 + 60, quiet=0)
    missing, dupes = harness.summarize(values, N)
    print(f"{kind}: rows={len(values)} missing={len(missing)} dupes={len(dupes)} reached={reached} "
          f"after {time.time() - t0:.0f}s state={harness.job(pid).get('state')}", flush=True)
    harness.set_stop(pid, "immediate")


if __name__ == "__main__":
    harness.start_arroyo("controller")
    files.ensure_namespace()
    for kind in sys.argv[1:] or ["json", "parquet", "iceberg"]:
        smoke(kind)
