"""Delivery invariants and faults.contract for every path, run concurrently.

Each scenario starts one pipeline per path, all reading the same input topic:
Kafka to Kafka (exactly-once and at-least-once), and Kafka to JSON files,
Parquet files and an Iceberg table. The three file paths have the shape of
Cloudflare Pipelines, which is built on Arroyo. One fault then hits all five
at once, as a node kill or a storage stall hits every pipeline on a cluster.
A producer writes 0..N-1, and each path's committed output must hold every
value: once, except at-least-once, which may repeat.

Time is scaled down, with Arroyo's clocks and the faults shrunk together, so
the same races happen in seconds:

| Clock                         | Default | Here | Fault that crosses it        |
|-------------------------------|---------|------|------------------------------|
| worker heartbeat timeout      | 30 s    | 12 s | worker_pause (16 s)          |
| Kafka transaction timeout     | 60 s    | 10 s | long stalls (15 s)           |
| file roll (Cloudflare min)    | 10 s    | 2 s  | (commit cadence)             |
| Iceberg roll (Cloudflare min) | 60 s    | 5 s  | (commit cadence)             |

A kill or stop that lands while an exactly-once Kafka sink is committing falls
in the gap its docs exempt (#224). That path's result for that scenario is
void, and its tests error instead of passing or failing.
"""

import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from confluent_kafka import Producer

import files
import harness

N, RATE = 20000, 2000
TXN_TIMEOUT_MS = 10_000
PAUSE = {"storage_outage": 6, "worker_pause": 16, "broker_pause": 6, "node_partition": 6,
         "long_storage_stall": 15, "long_broker_pause": 15}
ROLL = {"json": 2, "parquet": 2, "iceberg": 5}
FAULTS = ["worker_kill", "node_kill", "stop_with_checkpoint", "stop_immediately", "storage_outage",
          "worker_pause", "broker_pause", "node_partition", "long_storage_stall", "long_broker_pause",
          "kill_during_commit"]
DEPLOYS = ["quiet_restart", "idle_partition_restart"]
SUBJECTS = {
    "path.kafka_kafka_exactly_once": {"sink": "kafka", "mode": "exactly_once", "feature": "sink.kafka.exactly_once"},
    "path.kafka_kafka_at_least_once": {"sink": "kafka", "mode": "at_least_once", "feature": "sink.kafka.at_least_once"},
    "path.kafka_files_json": {"sink": "json", "feature": "sink.filesystem"},
    "path.kafka_files_parquet": {"sink": "parquet", "feature": "sink.filesystem"},
    "path.kafka_iceberg": {"sink": "iceberg", "feature": "sink.iceberg"},
}
# kill_during_commit is answered unsupported by the exactly-once Kafka path (#224).
SKIP = {("path.kafka_kafka_exactly_once", "kill_during_commit")}
FILES = {s for s, c in SUBJECTS.items() if c["sink"] != "kafka"}
AT_LEAST_ONCE = "path.kafka_kafka_at_least_once"
COMMIT_SENSITIVE = {"worker_kill", "node_kill", "stop_immediately", "node_partition"}


# --- one scenario, all paths -------------------------------------------------


def sink_ddl(subject, run):
    c = SUBJECTS[subject]
    if c["sink"] == "kafka":
        extra = (f", client_configs = 'transaction.timeout.ms={TXN_TIMEOUT_MS}'"
                 if c["mode"] == "exactly_once" else "")
        return (f"connector = 'kafka', type = 'sink', format = 'json', bootstrap_servers = "
                f"'{harness.KAFKA_INSIDE}', topic = '{run}-out', 'sink.commit_mode' = '{c['mode']}'{extra}")
    return files.ddl(c["sink"], run, ROLL[c["sink"]])


def start_all(scenario, src, offset):
    runs, pids = {}, {}
    for subject in SUBJECTS:
        if (subject, scenario) in SKIP:
            continue
        run = f"{scenario[:6]}-{subject.split('_', 1)[1][:9]}-{uuid.uuid4().hex[:5]}".replace("_", "-")
        if SUBJECTS[subject]["sink"] == "kafka":
            harness.create_topics(f"{run}-out")
        if SUBJECTS[subject]["sink"] == "iceberg":
            files.create_table(run)
        runs[subject] = run
        pids[subject] = harness.create_pipeline(run, f"""
CREATE TABLE src (seq BIGINT) WITH (connector = 'kafka', type = 'source', format = 'json',
    {offset}bootstrap_servers = '{harness.KAFKA_INSIDE}', topic = '{src}');
CREATE TABLE dst (seq BIGINT) WITH ({sink_ddl(subject, run)});
INSERT INTO dst SELECT seq FROM src;""")
    for subject, pid in pids.items():
        assert harness.wait_state(pid, {"Running"}) == "Running", f"{subject} never started"
    return runs, pids


def read(subject, run, n, timeout):
    """(values, reached, truncated)."""
    kind = SUBJECTS[subject]["sink"]
    if kind == "kafka":
        values, reached = harness.read_committed(f"{run}-out", "seq", n, timeout=timeout, quiet=5)
        return values, reached, None
    try:
        values, reached = files.wait_rows(kind, run, n, timeout, quiet=ROLL[kind] + 4)
        return values, reached, None
    except files.Truncated as e:
        return [], False, str(e)


def read_all(runs, n, timeout=90):
    with ThreadPoolExecutor(len(runs)) as ex:
        futures = {s: ex.submit(read, s, r, n, timeout) for s, r in runs.items()}
        return {s: f.result() for s, f in futures.items()}


def each(fn, items):
    with ThreadPoolExecutor(max(1, len(items))) as ex:
        return list(ex.map(fn, items))


def inject(fault, pids, jobs, rng):
    """Injects one fault into every pipeline. Returns (description, {job: host time of impact})."""
    if fault in ("worker_kill", "kill_during_commit"):
        by_job = harness.worker_pids_by_job()
        hits = {}
        for job in jobs:
            if not by_job.get(job):
                raise harness.FaultNotInjected(f"no worker for {job}")
            victim = rng.choice(by_job[job])
            hits[job] = time.time()
            harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -9 {victim}")
        time.sleep(1)
        left = harness.worker_pids_by_job()
        for job in jobs:
            if set(by_job[job]) <= set(left.get(job, [])):
                raise harness.FaultNotInjected(f"the worker for {job} survived kill -9")
        return f"killed one worker in each of {len(jobs)} pipelines", hits
    if fault == "worker_pause":
        by_job = harness.worker_pids_by_job()
        victims = [rng.choice(by_job[j]) for j in jobs if by_job.get(j)]
        if len(victims) != len(jobs):
            raise harness.FaultNotInjected("a pipeline had no worker to pause")
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -STOP {' '.join(victims)}")
        time.sleep(PAUSE[fault])
        harness.sh("docker", "exec", harness.CONTAINER, "sh", "-c", f"kill -CONT {' '.join(victims)}")
        return f"paused one worker in each pipeline {PAUSE[fault]}s", {}
    if fault == "node_kill":
        t = time.time()
        return harness.kill_node(rng), {j: t for j in jobs}
    if fault in ("stop_with_checkpoint", "stop_immediately"):
        mode = "checkpoint" if fault == "stop_with_checkpoint" else "immediate"
        t = time.time()
        each(lambda pid: harness.stop_and_resume(pid, mode), list(pids.values()))
        return f"stop {mode}, resumed", {j: t for j in jobs}
    if fault in ("storage_outage", "long_storage_stall"):
        return harness.pause_container(harness.STORAGE, PAUSE[fault]), {}
    if fault in ("broker_pause", "long_broker_pause"):
        return harness.pause_container(harness.KAFKA_CONTAINER, PAUSE[fault]), {}
    if fault == "node_partition":
        t = time.time()
        return harness.partition_node(PAUSE[fault]), {j: t for j in jobs}
    raise ValueError(fault)


def run_fault(fault):
    src = f"f-{fault}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    harness.create_topics(src)
    runs, pids = start_all(fault, src, "'source.offset' = 'earliest', ")
    jobs = {s: harness.job(p)["id"] for s, p in pids.items()}
    guard = harness.SleepGuard()
    watcher = harness.LogWatcher(set(jobs.values()))
    rng = harness.rng_for(src)
    producer = harness.SeqProducer(src, N, RATE)
    producer.start()
    try:
        time.sleep(rng.uniform(3, 5))
        desc, hits = inject(fault, pids, list(jobs.values()), rng)
        if fault == "node_kill":
            watcher.restart()
        each(lambda p: harness.wait_state(p, {"Running"}, timeout=60), list(pids.values()))
    finally:
        watcher.stop()
    producer.join()
    outputs = read_all(runs, N)
    states = {s: harness.job(p).get("state") for s, p in pids.items()}
    each(lambda p: harness.set_stop(p, "immediate"), list(pids.values()))
    guard.check()
    results = {}
    for s, (values, reached, truncated) in outputs.items():
        missing, dupes = harness.summarize(values, N)
        void = None
        if (SUBJECTS[s].get("mode") == "exactly_once" and fault in COMMIT_SENSITIVE
                and jobs[s] in hits and watcher.committing_at(hits[jobs[s]], jobs[s])):
            void = f"{desc}, but the sink was committing (the documented #224 gap)"
        results[s] = {"log": desc, "reached": reached, "state": states[s], "missing": missing,
                      "dupes": dupes, "rows": len(values), "truncated": truncated, "total": N, "void": void}
    return results


def write(topic, start, count, partition=None):
    p = Producer({"bootstrap.servers": harness.KAFKA, "enable.idempotence": True})
    kw = {} if partition is None else {"partition": partition}
    for i in range(start, start + count):
        p.produce(topic, key=str(i).encode(), value=harness.json.dumps({"seq": i}).encode(), **kw)
    p.flush(60)


def run_deploy(scenario):
    """Two deploys (stop with a checkpoint, resume) with the source's default offset."""
    first, later, idle_rows = 2000, 500, 100
    src = f"d-{scenario[:6]}-{uuid.uuid4().hex[:6]}".replace("_", "-")
    harness.create_topics(src)
    runs, pids = start_all(scenario, src, "")
    guard = harness.SleepGuard()
    time.sleep(4)  # the source assigns at `latest` first
    write(src, 0, first)
    first_out = read_all(runs, first, timeout=60)
    late = [s for s, (_, reached, _) in first_out.items() if not reached]
    assert not late, f"the first rows never arrived for {late}"
    each(lambda p: harness.stop_and_resume(p, "checkpoint"), list(pids.values()))
    each(lambda p: harness.wait_state(p, {"Running"}), list(pids.values()))
    total = first + later
    if scenario == "idle_partition_restart":
        write(src, total, idle_rows, partition=0)
        total += idle_rows
    time.sleep(6)  # a few 2 s checkpoints

    def stop(pid):
        harness.set_stop(pid, "checkpoint")
        if harness.wait_state(pid, {"Stopped", "Failed"}) != "Stopped":
            raise harness.FaultNotInjected("the second stop did not reach Stopped")
    each(stop, list(pids.values()))
    write(src, first, later)
    each(lambda p: harness.set_stop(p, "none"), list(pids.values()))
    each(lambda p: harness.wait_state(p, {"Running"}), list(pids.values()))
    outputs = read_all(runs, total, timeout=60)
    each(lambda p: harness.set_stop(p, "immediate"), list(pids.values()))
    guard.check()
    results = {}
    for s, (values, reached, truncated) in outputs.items():
        missing, dupes = harness.summarize(values, total)
        results[s] = {"log": scenario, "reached": reached, "state": "n/a", "missing": missing, "dupes": dupes,
                      "rows": len(values), "truncated": truncated, "total": total, "void": None}
    return results


_cache = {}


def result(subject, case):
    if case not in _cache:
        try:
            _cache[case] = run_deploy(case) if case in DEPLOYS else run_fault(case)
        except harness.HostSlept as e:
            pytest.exit(f"run void: {e}", returncode=3)
        except Exception as e:
            # A scenario runs once. If it breaks, every test that reads it
            # reports the same error instead of running it again.
            _cache[case] = e
            print(f"\n[scenario {case}] raised {type(e).__name__}: {e}")
        if not isinstance(_cache[case], Exception):
            for s, r in _cache[case].items():
                print(f"\n[{s} {case}] {r['log']} state={r['state']} rows={r['rows']} want={r['total']} "
                      f"missing={len(r['missing'])} dupes={len(r['dupes'])} reached={r['reached']} "
                      f"truncated={r['truncated']} void={r['void']}")
    if isinstance(_cache[case], Exception):
        raise _cache[case]
    r = _cache[case][subject]
    if r["void"]:
        raise harness.LandedInCommitPhase(r["void"])
    return r


# --- tests -------------------------------------------------------------------


def cases(refs_for, subjects=SUBJECTS, which=FAULTS + DEPLOYS):
    out = []
    for case in which:  # scenario-major, so each scenario runs once
        for subject in subjects:
            if (subject, case) in SKIP:
                continue
            marks = [pytest.mark.covers(ref, subject=subject) if ref.startswith(("faults.", "delivery.", "files."))
                     else pytest.mark.covers(ref) for ref in refs_for(subject, case)]
            out.append(pytest.param(subject, case, id=f"{case}-{subject.split('.')[1]}", marks=marks))
    return out


def lost(r):
    return f"{len(r['missing'])} of {r['total']} rows missing, e.g. {r['missing'][:10]}; {r['log']}"


def twice(r):
    return f"{len(r['dupes'])} rows delivered twice, e.g. {r['dupes'][:10]}; {r['log']}"


@pytest.mark.parametrize("subject,case", cases(
    lambda s, c: [f"faults.contract/{c}", "source.kafka", SUBJECTS[s]["feature"]]))
def test_case_keeps_every_delivery_invariant(subject, case):
    r = result(subject, case)
    assert not r["truncated"], r["truncated"]
    assert not r["missing"], lost(r)
    if subject != AT_LEAST_ONCE:
        assert not r["dupes"], twice(r)
    if case in FAULTS:
        assert r["reached"] and r["state"] == "Running", f"no recovery; state {r['state']}; {r['log']}"


@pytest.mark.parametrize("subject,case", cases(lambda s, c: ["delivery.no_loss"]))
def test_no_loss(subject, case):
    r = result(subject, case)
    assert not r["missing"], lost(r)


@pytest.mark.parametrize("subject,case", cases(
    lambda s, c: ["delivery.no_duplicates"], [s for s in SUBJECTS if s != AT_LEAST_ONCE]))
def test_no_duplicates(subject, case):
    r = result(subject, case)
    assert not r["dupes"], twice(r)


@pytest.mark.parametrize("subject,case", cases(lambda s, c: ["delivery.recovers"], which=FAULTS))
def test_recovers(subject, case):
    r = result(subject, case)
    assert r["reached"] and r["state"] == "Running", f"output did not catch up; state {r['state']}; {r['log']}"


@pytest.mark.parametrize("subject,case", cases(lambda s, c: ["files.complete"], sorted(FILES)))
def test_files_complete(subject, case):
    r = result(subject, case)
    assert not r["truncated"], r["truncated"]


@pytest.mark.exempts("delivery.no_duplicates", subject=AT_LEAST_ONCE)
def test_at_least_once_loses_nothing():
    """At-least-once may repeat rows after a failure, as its docs say, but never loses one."""
    r = result(AT_LEAST_ONCE, "worker_kill")
    assert not r["missing"], lost(r)
