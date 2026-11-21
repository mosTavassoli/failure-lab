"""Run the CPU scheduling drill in a fresh, disposable single-node kind cluster."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from http.client import HTTPConnection
from pathlib import Path
from urllib.request import urlopen
from uuid import UUID, uuid4

from evidence import (
    BROKER, GROUP, NAMESPACE, PARTITIONS, SCALED_OBJECT, TARGET, TOPIC, WORKER,
    confirms_cpu_block, kafka_lag, milli_units, observed_pods, scaling_observations,
    scheduler_message_for_replay, verify_tickets, whole_units,
)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
NODE_IMAGE = "kindest/node:v1.34.3@sha256:08497ee19eace7b4b5348db5c6a1591d7752b164530a36f855cb0f2bdcbadd48"
KEDA_URL = "https://github.com/kedacore/keda/releases/download/v2.20.0/keda-2.20.0.yaml"
KEDA_SHA = "b7900d5f9b5dd01750961a01abc5dcbe45c555cdf2e846be71c8cdf81efdfb40"
PAUSE_IMAGE = "registry.k8s.io/pause:3.10.1"
SALE = "f0000000-0000-4000-8000-000000000065"
BURST = 64
MIN_TEMP_FREE = 5 * 1024**3


def now():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def command(*args, data=None, capture=True, timeout=30):
    result = subprocess.run(args, cwd=ROOT, input=data, text=True, encoding="utf-8",
                            stdout=subprocess.PIPE if capture else None, stderr=subprocess.PIPE if capture else None,
                            timeout=timeout, check=False)
    if result.returncode:
        detail = (result.stderr or result.stdout or "").strip() or f"exit {result.returncode}"
        raise RuntimeError(f"{' '.join(map(str, args[:4]))}: {detail}")
    return result.stdout


def wait_for(label, read, predicate, timeout):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = read()
            if predicate(value):
                return value
            last = value
        except (OSError, RuntimeError, ValueError, KeyError, subprocess.TimeoutExpired) as exc:
            last = exc
        time.sleep(1)
    raise RuntimeError(f"Timed out waiting for {label}. Last read: {str(last)[:1800]}")


class Lab:
    def __init__(self, name, directory, kind="kind"):
        self.name = name
        self.directory = directory
        self.kind = kind
        self.config = directory / "kubeconfig"
        self.base = ("kubectl", "--kubeconfig", str(self.config), "--context", f"kind-{name}")

    def kube(self, *args, ns=NAMESPACE, **kwargs):
        return command(*self.base, *(("-n", ns) if ns else ()), *args, **kwargs)

    def get(self, *args, **kwargs):
        return json.loads(self.kube("get", *args, "-o", "json", **kwargs))

    def show_failure(self):
        if not self.config.is_file():
            return
        for namespace in (NAMESPACE, "keda"):
            for args in (("pods",), ("events", "--field-selector=type=Warning")):
                try:
                    print(self.kube("get", *args, ns=namespace, timeout=8), file=sys.stderr, end="")
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    pass  # Keep the original failure if the lab API is unavailable.

    def build_and_load(self):
        images = []
        for app in ("reservation-api", WORKER):
            image = f"failure-lab/{app}:run"
            if not (ROOT / app / "target/quarkus-app/quarkus-run.jar").is_file():
                raise RuntimeError("Package the two apps with Maven first (see lab/kind/README.md)")
            command("docker", "build", "--provenance=false", "-f", HERE / "Dockerfile", "-t", image,
                    ROOT / app / "target/quarkus-app", capture=False, timeout=240)
            images.append(image)
        command(self.kind, "load", "docker-image", "--name", self.name, *images, capture=False, timeout=240)

    def install(self, keda):
        self.kube("apply", "--server-side", "-f", keda, ns=None, timeout=65)
        self.kube("wait", "--for=condition=Available", "deployment", "--all", "--timeout=180s",
                  ns="keda", timeout=190)
        self.kube("wait", "--for=condition=Available", "apiservice/v1beta1.external.metrics.k8s.io",
                  "--timeout=90s", ns=None, timeout=100)
        self.kube("apply", "-f", HERE / "workload.yaml", ns=None)
        self.kube("wait", "--for=condition=Available", "deployment", "--all", "--timeout=120s", timeout=130)
        self.broker("kafka-topics", "--create", "--topic", TOPIC, "--partitions", str(PARTITIONS),
                    "--replication-factor", "1")
        self.kube("apply", "-f", HERE / "scaled-object.yaml")

    def broker(self, executable, *args):
        return self.kube("exec", "deploy/kafka", "--", f"/opt/kafka/bin/{executable}.sh",
                         "--bootstrap-server", BROKER, *args, timeout=20)

    def lag(self):
        return kafka_lag(self.broker("kafka-consumer-groups", "--describe", "--offsets", "--group", GROUP))

    def sql(self, statement):
        return self.kube("exec", "deploy/postgres", "--", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                         "-U", "lab", "-d", "failurelab", "-c", statement)

    @contextmanager
    def port_forward(self):
        path = self.directory / "port-forward.log"
        with path.open("wb") as log:
            process = subprocess.Popen((*self.base, "-n", NAMESPACE, "port-forward",
                                        "service/reservation-api", ":8080", "--address", "127.0.0.1"),
                                       stdout=log, stderr=subprocess.STDOUT)
            try:
                pattern = re.compile(r"Forwarding from 127\.0\.0\.1:(\d+) -> 8080")
                match = wait_for("local API port", lambda: pattern.search(path.read_text(encoding="utf-8")),
                                 bool, 20)
                yield int(match[1])
            finally:
                process.terminate()
                process.wait(timeout=10)

    def reserve_cpu(self):
        nodes = self.get("nodes", ns=None)["items"]
        if len(nodes) != 1 or nodes[0]["metadata"]["name"] != f"{self.name}-control-plane":
            raise RuntimeError("Expected a single fresh kind control-plane node")
        node, = nodes
        allocatable = milli_units(node["status"]["allocatable"]["cpu"])
        worker = self.get("deploy", WORKER)["spec"]["template"]["spec"]["containers"][0]
        request = milli_units(worker["resources"]["requests"]["cpu"])
        description = self.kube("describe", "node", node["metadata"]["name"], ns=None)
        # kubectl's aggregate of active pod requests accounts for init containers too.
        match = re.search(r"(?m)^\s+cpu\s+([0-9]+m?)\s+\(\d+%\)",
                          description.split("Allocated resources:", 1)[-1])
        if not match:
            raise RuntimeError("Cannot read kubectl's aggregate CPU requests")
        requested = milli_units(match[1])
        spare = request // 2
        ballast = allocatable - requested - spare
        if allocatable - requested < (PARTITIONS - 1) * request or ballast <= 0:
            raise RuntimeError("Not enough allocatable CPU to schedule four lab workers after release")
        self.kube("create", "-f", "-", data=json.dumps({
            "apiVersion": "v1", "kind": "Pod", "metadata": {"name": "cpu-reservation"},
            "spec": {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 0, "containers": [
                {"name": "pause", "image": PAUSE_IMAGE, "resources": {"requests": {"cpu": f"{ballast}m"}}}
            ]},
        }))
        self.kube("wait", "--for=condition=Ready", "pod/cpu-reservation", "--timeout=45s", timeout=50)
        return {"allocatable_mcpu": allocatable, "previously_requested_mcpu": requested,
                "reserved_mcpu": ballast, "worker_request_mcpu": request, "remaining_mcpu": spare}

    def worker(self):
        return self.get("deploy", WORKER)

    def scaling(self):
        scaled = self.get("scaledobject", SCALED_OBJECT)
        scaled_at = now()
        hpa = self.get("hpa", scaled["status"]["hpaName"])
        hpa_at = now()
        deployment = self.worker()
        deployment_at = now()
        items = self.get("replicasets,pods", "-l", f"app={WORKER}")["items"]
        observations = scaling_observations(scaled, hpa, deployment, scaled_at)
        observations["hpa"]["observed_at"] = hpa_at
        observations["deployment"]["observed_at"] = deployment_at
        observations["pods"] = {"observed_at": now(), "target": TARGET, "items": observed_pods(deployment, items)}
        return observations

    def metric(self, metric):
        selector = f"labelSelector=scaledobject.keda.sh%2Fname%3D{SCALED_OBJECT}"
        path = f"/apis/external.metrics.k8s.io/v1beta1/namespaces/{NAMESPACE}/{metric}?{selector}"
        item, = json.loads(self.kube("get", "--raw", path, ns=None))["items"]
        labels = item.get("metricLabels") or {}
        if item["metricName"] != metric or labels.get("scaledobject.keda.sh/name", SCALED_OBJECT) != SCALED_OBJECT:
            raise ValueError("External metric scope differs from the lab ScaledObject")
        return {"observed_at": now(), "namespace": NAMESPACE, "scaled_object": SCALED_OBJECT, "metric": metric,
                "value": whole_units(item["value"])}

    def ledger(self):
        return json.loads(self.sql(f"""
            SELECT jsonb_build_object(
              'reservations', COALESCE((SELECT jsonb_agg(to_jsonb(acceptance) ORDER BY id) FROM (
                  SELECT r.id, r.status, o.event_id
                  FROM reservation r LEFT JOIN outbox o
                    ON o.reservation_id = r.id AND o.event_type = '{TOPIC}'
              ) acceptance), '[]'),
              'tickets', COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY reservation_id, id)
                                    FROM (SELECT id, reservation_id, source_event_id FROM ticket) t), '[]')
            )
            """))


def api(port, method, path, body=None):
    connection = HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        headers = {} if body is None else {"Content-Type": "application/json", "Idempotency-Key": str(uuid4())}
        connection.request(method, path, body, headers)
        response = connection.getresponse()
        text = response.read().decode()
        if response.status != (201 if method == "POST" else 200):
            raise RuntimeError(f"{method} {path}: HTTP {response.status} {text[:300]}")
        return json.loads(text)
    finally:
        connection.close()


def reserve(port):
    row = api(port, "POST", "/api/reservations", json.dumps({"saleId": SALE}))
    if row["saleId"] != SALE:
        raise ValueError("The API accepted a reservation for a different sale")
    return str(UUID(row["id"]))


def run_drill(lab, out):
    timeline = []

    def record(stage, **facts):
        timeline.append({"observed_at": now(), "stage": stage, **facts})
        print(f"{stage}: {facts}", flush=True)

    lab.sql(f"""
        INSERT INTO sale (id, name, capacity, remaining)
        VALUES ('{SALE}', 'CPU scheduling drill', {BURST + 1}, {BURST + 1})
        """)
    with lab.port_forward() as port:
        baseline = reserve(port)
        wait_for("healthy reservation to issue",
                 lambda: api(port, "GET", f"/api/reservations/{baseline}")["status"],
                 lambda state: state == "ISSUED", 60)
        initial = wait_for("one desired and ready worker", lab.scaling,
                           lambda obs: obs["hpa"]["desired_replicas"] == obs["deployment"]["desired_replicas"]
                           == obs["deployment"]["ready_replicas"] == 1, 60)
        initial_lag = wait_for("committed baseline offset", lab.lag, lambda lag: lag == 0, 40)
        healthy = verify_tickets([baseline], lab.ledger())
        if not healthy["passed"]:
            raise RuntimeError(f"Baseline reservation did not reconcile: {healthy}")
        record("baseline", hpa_desired=initial["hpa"]["desired_replicas"],
               ready=initial["deployment"]["ready_replicas"], lag=initial_lag,
               accepted=healthy["accepted"], tickets=healthy["tickets"])

        record("cpu_reserved", **lab.reserve_cpu())
        with ThreadPoolExecutor(max_workers=8) as clients:
            burst = list(clients.map(reserve, [port] * BURST))
        accepted = [baseline, *burst]
        record("burst_accepted", count=len(burst))

        observations = wait_for("CPU-only Pending worker replicas", lab.scaling, confirms_cpu_block, 85)
        lag = lab.lag()
        observations["kafka"] = {
            "observed_at": now(), "kafka_cluster": observations["scaled_object"]["kafka_cluster"],
            "topic": TOPIC, "consumer_group": GROUP, "partitions": PARTITIONS, "lag": lag,
        }
        metric = lab.metric(observations["hpa"]["metric"])
        if min(lag, metric["value"]) <= 0:
            raise RuntimeError("There must still be Kafka demand during the blocked scale-out")
        observations["external_metric"] = metric
        # Keep the original PodScheduled messages in the run evidence. The
        # compatibility snapshot can omit known post-filter commentary.
        raw_messages = {pod["name"]: pod["message"] for pod in observations["pods"]["items"]
                        if pod["scheduled"] is False}
        for pod in observations["pods"]["items"]:
            pod["message"] = scheduler_message_for_replay(pod["message"])
        snapshot = {"format": "kafka-oncall/snapshot-v1", "id": "kind-cpu-scheduling",
                    "target": TARGET, "as_of": now(), "observations": observations}
        as_of = datetime.fromisoformat(snapshot["as_of"])
        if any(not 0 <= (as_of - datetime.fromisoformat(obs["observed_at"])).total_seconds() <= 30
               for obs in observations.values()):
            raise RuntimeError("Incident observations crossed the Kafka On-Call freshness window")
        write_json(out / "incident.snapshot.json", snapshot)
        desired = observations["hpa"]["desired_replicas"]
        record("cpu_blocked", hpa_desired=desired, ready=observations["deployment"]["ready_replicas"],
               lag=lag, external_metric=metric["value"], evidence="incident.snapshot.json",
               scheduler_messages=raw_messages)

        lab.kube("delete", "pod/cpu-reservation", "--timeout=30s", timeout=35)
        record("cpu_released")
        recovered = wait_for("requested workers ready after releasing CPU", lab.worker,
                             lambda deploy: deploy["status"].get("readyReplicas", 0) >= desired, 75)
        record("workers_ready", ready=recovered["status"]["readyReplicas"])
        final_lag = wait_for("Kafka consumer lag to drain", lab.lag, lambda lag: lag == 0, 150)
        record("lag_drained", lag=final_lag)
        ledger = lab.ledger()
        result = verify_tickets(accepted, ledger)
        record("tickets_reconciled", passed=result["passed"], accepted=result["accepted"], tickets=result["tickets"])
        write_json(out / "run.json", {
            "format": "failure-lab/kind-cpu-run-v1", "scenario": snapshot["id"], "target": TARGET,
            "components": {"kubernetes": "1.34.3", "keda": "2.20.0", "kafka": "4.1.1", "postgresql": "17"},
            "timeline": timeline, "accepted_ids": sorted(accepted), "ledger": ledger, "verification": result,
        })
        if not result["passed"]:
            raise RuntimeError(f"Kafka drained but ticket verification failed: {result}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", default="kind", help="path to the kind binary, if not on PATH")
    parser.add_argument("--out", type=Path, required=True, help="new directory for normalized evidence")
    args = parser.parse_args()
    if args.out.exists():
        parser.error("--out must be a new directory")
    local_data = os.environ.get("LOCALAPPDATA")
    parent = Path(local_data) / "Temp/opencode" if local_data else None
    # The kind CLI also archives local images in the host temp directory.
    temp_dirs = {Path(tempfile.gettempdir())}
    if parent and parent.is_dir():
        temp_dirs.add(parent)
    else:
        parent = None
    for path in temp_dirs:
        if shutil.disk_usage(path).free < MIN_TEMP_FREE:
            parser.error(f"free at least 5 GiB on the drive containing {path} before running the drill")
    try:
        docker_os = command("docker", "info", "--format", "{{.OSType}}", timeout=15).strip()
    except FileNotFoundError:
        parser.error("Docker CLI not found; install Docker and add it to PATH")
    except (OSError, RuntimeError, subprocess.TimeoutExpired):
        parser.error("Docker is not responding; check disk space and make sure `docker info` works")
    if docker_os != "linux":
        parser.error("use a local Docker engine running Linux containers")

    with tempfile.TemporaryDirectory(prefix="failure-lab-kind-", dir=parent) as temp:
        directory = Path(temp)
        lab = Lab("failure-lab-cpu-" + uuid4().hex[:16], directory, args.kind)
        kind = ("--name", lab.name, "--kubeconfig", lab.config)
        print(f"Using disposable kind cluster {lab.name}", flush=True)
        manifest = directory / "keda.yaml"
        with urlopen(KEDA_URL, timeout=30) as response:
            content = response.read(1024 * 1024)
        if hashlib.sha256(content).hexdigest() != KEDA_SHA:
            raise RuntimeError("KEDA release manifest checksum mismatch")
        manifest.write_bytes(content)
        args.out.mkdir(parents=True, exist_ok=False)
        try:
            command(lab.kind, "create", "cluster", *kind, "--image", NODE_IMAGE, "--wait", "120s",
                    capture=False, timeout=360)
            lab.build_and_load()
            lab.install(manifest)
            run_drill(lab, args.out)
        except Exception:
            lab.show_failure()
            raise
        finally:
            command(lab.kind, "delete", "cluster", *kind, capture=False, timeout=120)


if __name__ == "__main__":
    main()
