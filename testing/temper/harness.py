"""Docker, Kafka and Arroyo helpers for the Temper tests.

Kafka (temper-kafka) and SeaweedFS (temper-seaweed) run on the temper-arroyo
network, as temper/README.md shows. Each test session starts its own Arroyo
container, in the control-plane mode its level names.
"""

import json
import re
import os
import random
import subprocess
import threading
import time
import uuid
from collections import Counter

import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

IMAGE = os.environ.get("TEMPER_ARROYO_IMAGE", "ghcr.io/arroyosystems/arroyo:0.16.0")
CONTAINER = "temper-arroyo"
STORAGE = "temper-seaweed"
API = "http://localhost:25115/api/v1"
KAFKA = "localhost:29092"  # from the host
KAFKA_INSIDE = "kafka:9092"  # from Arroyo
MODES = {"controller": "controller", "worker_leader": "worker"}


def sh(*args, check=False):
    return subprocess.run(args, capture_output=True, text=True, check=check)


# --- Arroyo -----------------------------------------------------------------


def start_arroyo(mode):
    sh("docker", "rm", "-f", CONTAINER)
    sh("docker", "run", "-d", "--name", CONTAINER, "--network", "temper-arroyo", "-p", "25115:5115",
       "-e", f"ARROYO__JOB_CONTROLLER={mode}",
       "-e", f"ARROYO__CHECKPOINT_URL=s3://arroyo/checkpoints-{uuid.uuid4().hex[:8]}",
       "-e", "AWS_ENDPOINT=http://seaweed:8333", "-e", "AWS_ALLOW_HTTP=true",
       "-e", "AWS_ACCESS_KEY_ID=temper", "-e", "AWS_SECRET_ACCESS_KEY=temper", "-e", "AWS_REGION=us-east-1",
       IMAGE, "cluster", check=True)
    wait_api()


def wait_api(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{API}/ping", timeout=2).ok:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    raise RuntimeError("Arroyo API never came up")


def create_pipeline(name, query, parallelism=2, checkpoint_seconds=2):
    r = requests.post(f"{API}/pipelines", json={
        "name": name, "query": query, "parallelism": parallelism,
        "checkpoint_interval_micros": checkpoint_seconds * 1_000_000,
    })
    if not r.ok:
        raise RuntimeError(f"create pipeline: {r.status_code} {r.text}")
    return r.json()["id"]


def job(pid):
    try:
        jobs = requests.get(f"{API}/pipelines/{pid}/jobs", timeout=5).json()["data"]
    except (requests.RequestException, ValueError, KeyError):
        return {}
    return jobs[0] if jobs else {}


def wait_state(pid, states, timeout=120):
    deadline = time.time() + timeout
    state = None
    while time.time() < deadline:
        state = job(pid).get("state")
        if state in states:
            return state
        time.sleep(0.5)
    return state


def set_stop(pid, mode):
    r = requests.patch(f"{API}/pipelines/{pid}", json={"stop": mode}, timeout=10)
    r.raise_for_status()


def worker_pids():
    out = sh("docker", "exec", CONTAINER, "sh", "-c",
             'for p in /proc/[0-9]*; do [ "$(cat $p/comm 2>/dev/null)" = arroyo ] && '
             'grep -q worker $p/cmdline 2>/dev/null && echo ${p#/proc/}; done').stdout
    return out.split()


# --- faults -----------------------------------------------------------------


class FaultNotInjected(RuntimeError):
    """A fault that did not take effect. A test must never pass on one."""


class HostSlept(RuntimeError):
    """The host slept during a run, pausing Docker. The run proves nothing."""


class SleepGuard:
    """On macOS the monotonic clock stops while the host sleeps; the wall clock does not."""

    def __init__(self):
        self.wall, self.mono = time.time(), time.monotonic()

    def check(self, slack=5.0):
        drift = (time.time() - self.wall) - (time.monotonic() - self.mono)
        if drift > slack:
            raise HostSlept(f"the host slept for about {drift:.0f}s during this run")


class LandedInCommitPhase(RuntimeError):
    """A fault meant for outside the commit phase landed inside it.

    The docs exempt failures during the commit phase (#224), so a run like this
    proves nothing about the fault under test. The test errors instead of
    passing or failing.
    """


CHECKPOINT_LINE = re.compile(r'(Starting checkpointing|Finished checkpointing|Committing checkpoint|'
                             r'Finished committing checkpointing) job_id="(\w+)" epoch=(\d+)')


class LogWatcher:
    """Follows Arroyo's log and stamps each checkpoint line with the host clock.

    Faults are timed against these stamps, so the host and container clocks
    never need to agree.
    """

    def __init__(self, job_id):
        self.job_id = job_id
        self.events = []  # (host time, kind, epoch)
        self._start()

    def _start(self):
        self.proc = subprocess.Popen(["docker", "logs", "-f", "--since", "2s", CONTAINER],
                                     stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        threading.Thread(target=self._read, args=(self.proc,), daemon=True).start()

    def _read(self, proc):
        for raw in proc.stdout:
            m = CHECKPOINT_LINE.search(ANSI.sub("", raw))
            if m and m.group(2) == self.job_id:
                self.events.append((time.time(), m.group(1), int(m.group(3))))

    def restart(self):
        self.proc.kill()
        self._start()

    def stop(self):
        self.proc.kill()

    def committing_at(self, t):
        """True if a commit had started and not finished at host time t."""
        state = False
        for at, kind, _ in self.events:
            if at > t:
                break
            if kind == "Committing checkpoint":
                state = True
            elif kind == "Finished committing checkpointing":
                state = False
        return state

    def wait_outside_commit(self, low=0.3, high=1.2, timeout=60):
        """Waits until `low` to `high` seconds after a commit finished."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            # A job with no committing operator logs no commit lines. Then any
            # moment after a finished checkpoint is outside the commit phase.
            commits = any(kind == "Committing checkpoint" for _, kind, _ in self.events)
            marker = "Finished committing checkpointing" if commits else "Finished checkpointing"
            done = [at for at, kind, _ in self.events if kind == marker]
            if done and low <= time.time() - done[-1] <= high and not self.committing_at(time.time()):
                return
            time.sleep(0.02)
        raise FaultNotInjected("no commit finished within the timeout, so no safe moment to inject")


ANSI = re.compile(r"\x1b\[[0-9;]*m")


def kill_worker(rng):
    pids = worker_pids()
    if not pids:
        raise FaultNotInjected("no worker process to kill")
    victim = rng.choice(pids)
    # The image has no kill binary, so use the shell's builtin.
    sh("docker", "exec", CONTAINER, "sh", "-c", f"kill -9 {victim}")
    for _ in range(20):
        if victim not in worker_pids():
            return f"killed worker pid {victim}"
        time.sleep(0.1)
    raise FaultNotInjected(f"worker pid {victim} survived kill -9")


def started_at():
    return sh("docker", "inspect", "-f", "{{.State.StartedAt}}", CONTAINER).stdout.strip()


def kill_node(rng):
    before = started_at()
    sh("docker", "kill", "-s", "KILL", CONTAINER)
    time.sleep(1)
    sh("docker", "start", CONTAINER)
    if started_at() == before:
        raise FaultNotInjected("the node did not restart")
    wait_api()
    return "killed and restarted the node"


def stop_and_resume(pid, mode):
    set_stop(pid, mode)
    state = wait_state(pid, {"Stopped", "Failed"}, timeout=120)
    if state != "Stopped":
        raise FaultNotInjected(f"stop {mode} reached {state}, not Stopped")
    set_stop(pid, "none")
    return f"stop {mode}, resumed"


def pause_storage(seconds=15):
    sh("docker", "pause", STORAGE)
    time.sleep(seconds)
    sh("docker", "unpause", STORAGE)
    return f"paused storage {seconds}s"


def inject(fault, pid, rng, watcher):
    """Injects one fault. Kills are timed outside the commit phase and checked after."""
    if fault in ("worker_kill", "node_kill"):
        watcher.wait_outside_commit()
        at = time.time()
        msg = kill_worker(rng) if fault == "worker_kill" else kill_node(rng)
        if fault == "node_kill":
            watcher.restart()
        if watcher.committing_at(at):
            raise LandedInCommitPhase(f"{msg}, but a commit was in progress")
        return msg
    if fault == "kill_during_commit":
        # Only asked of paths with no commit phase (at-least-once): a kill at
        # any moment must lose nothing.
        time.sleep(rng.uniform(0, 2))
        return kill_worker(rng) + " at a random moment"
    if fault == "stop_with_checkpoint":
        return stop_and_resume(pid, "checkpoint")
    if fault == "stop_immediately":
        return stop_and_resume(pid, "immediate")
    if fault == "storage_outage":
        return pause_storage()
    raise ValueError(fault)


# --- Kafka ------------------------------------------------------------------


def create_topics(*names, partitions=4):
    admin = AdminClient({"bootstrap.servers": KAFKA})
    for f in admin.create_topics([NewTopic(n, num_partitions=partitions, replication_factor=1) for n in names]).values():
        f.result(timeout=30)


class SeqProducer(threading.Thread):
    """Writes {"seq": i} for i in 0..n-1 at about `rate` messages a second."""

    def __init__(self, topic, n, rate):
        super().__init__(daemon=True)
        self.topic, self.n, self.rate = topic, n, rate
        self.produced = 0

    def run(self):
        p = Producer({"bootstrap.servers": KAFKA, "linger.ms": 5, "enable.idempotence": True})
        start = time.time()
        for i in range(self.n):
            p.produce(self.topic, key=str(i).encode(), value=json.dumps({"seq": i}).encode())
            self.produced += 1
            if i % 500 == 0:
                p.poll(0)
                ahead = i / self.rate - (time.time() - start)
                if ahead > 0:
                    time.sleep(ahead)
        p.flush(60)


def read_committed(topic, field, until_count, timeout, quiet=10):
    """Reads a topic as a read_committed consumer would.

    Returns (values, reached): every `field` value seen, and whether the count
    reached `until_count` before `timeout`. After reaching it, keeps reading
    for `quiet` seconds so late duplicates are seen too.
    """
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": f"temper-{uuid.uuid4().hex}",
                  "isolation.level": "read_committed", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([topic])
    values, reached_at = [], None
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = c.poll(0.5)
            if msg is not None and not msg.error():
                values.append(json.loads(msg.value())[field])
            if reached_at is None and len(set(values)) >= until_count:
                reached_at = time.time()
            if reached_at is not None and time.time() - reached_at > quiet:
                break
    finally:
        c.close()
    return values, reached_at is not None


def summarize(values, n):
    counts = Counter(values)
    missing = sorted(set(range(n)) - set(counts))
    dupes = sorted(v for v, k in counts.items() if k > 1)
    return missing, dupes


def rng_for(name):
    return random.Random(name)
