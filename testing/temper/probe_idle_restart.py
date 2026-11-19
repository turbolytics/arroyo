"""Private probe: does a restart with no new input lose the Kafka source's offsets?

No faults. Each trial:
1. Writes N rows, and waits until the pipeline copied them all.
2. Stops the pipeline with a checkpoint and resumes it, twice, writing nothing.
   (Two deploys in a quiet period.)
3. Writes M new rows and reads the output.

Expected: N + M rows, each once. If the second resume starts from
`source.offset`, the output repeats the first N rows (earliest) or drops the
new ones written before it caught up (latest).

usage: probe_idle_restart.py MODE TRIALS   (MODE: exactly_once | at_least_once)
"""

import re
import sys
import time
import uuid

import harness

N, M = 2000, 500


def write(topic, start, count):
    p = harness.SeqProducer(topic, count, 10_000)
    p.n_offset = start
    p.start()
    p.join()


class Seq(harness.SeqProducer):
    def __init__(self, topic, start, count):
        super().__init__(topic, count, 10_000)
        self.start_at = start

    def run(self):
        from confluent_kafka import Producer
        p = Producer({"bootstrap.servers": harness.KAFKA, "enable.idempotence": True})
        for i in range(self.start_at, self.start_at + self.n):
            p.produce(self.topic, key=str(i).encode(), value=harness.json.dumps({"seq": i}).encode())
        p.flush(60)


def trial(mode, i):
    name = f"idle-{mode[:2]}-{i}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    src, dst = f"{name}-in", f"{name}-out"
    harness.create_topics(src, dst)
    pid = harness.create_pipeline(name, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}', 'source.offset' = 'earliest');
CREATE TABLE dst (seq BIGINT) WITH (connector = 'kafka', type = 'sink', format = 'json',
    bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{dst}', 'sink.commit_mode' = '{mode}');
INSERT INTO dst SELECT seq FROM src;""")
    assert harness.wait_state(pid, {"Running"}) == "Running"
    w = Seq(src, 0, N)
    w.start()
    w.join()
    _, reached = harness.read_committed(dst, "seq", N, timeout=60, quiet=6)
    assert reached, "first batch never arrived"
    for _ in range(2):
        harness.stop_and_resume(pid, "checkpoint")
        assert harness.wait_state(pid, {"Running"}) == "Running"
        time.sleep(8)  # several 2 s checkpoints with no input
    w = Seq(src, N, M)
    w.start()
    w.join()
    values, reached = harness.read_committed(dst, "seq", N + M, timeout=60, quiet=10)
    missing, dupes = harness.summarize(values, N + M)
    job_id = harness.job(pid)["id"]
    maps = [re.sub(r"^.*partition map for ", "", harness.ANSI.sub("", l))[:150]
            for l in (harness.sh("docker", "logs", harness.CONTAINER).stdout +
                      harness.sh("docker", "logs", harness.CONTAINER).stderr).splitlines()
            if f"partition map for {src}" in l]
    harness.set_stop(pid, "immediate")
    print(f"{mode} trial {i}: rows={len(values)} (want {N + M}) missing={len(missing)} "
          f"duplicated={len(dupes)}" + (f" ({dupes[0]}..{dupes[-1]})" if dupes else ""), flush=True)
    seen = []
    for m in maps:
        if m not in seen:
            seen.append(m)
    for m in seen:
        print("    source start:", m, flush=True)


if __name__ == "__main__":
    mode, trials = sys.argv[1], int(sys.argv[2])
    harness.start_arroyo("controller")
    for i in range(trials):
        trial(mode, i)
