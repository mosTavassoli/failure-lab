"""Normalize evidence and verify a saved ticket ledger without cluster access."""

import argparse
import json
import re
from collections import Counter
from pathlib import Path

NAMESPACE = "failure-lab"
WORKER = "fulfillment-worker"
SCALED_OBJECT = "fulfillment"
TARGET = f"{NAMESPACE}/{WORKER}"
TOPIC = "reservation.accepted.v1"
GROUP = "failure-lab-fulfillment"
BROKER = "kafka.failure-lab.svc.cluster.local:9092"
PARTITIONS = 4
CPU_ONLY = re.compile(r"0/1 nodes are available: 1 Insufficient cpu\.?")
POST_FILTER_DRA = ". no new claims to deallocate, preemption:"


def milli_units(quantity):
    """This fixture uses canonical whole-unit or milli-unit Kubernetes quantities."""
    if not re.fullmatch(r"[0-9]+m?", quantity):
        raise ValueError(f"Unexpected Kubernetes quantity: {quantity}")
    return int(quantity[:-1]) if quantity.endswith("m") else 1000 * int(quantity)


def whole_units(quantity):
    milli = milli_units(quantity)
    if milli % 1000:
        raise ValueError(f"Expected an integer metric: {quantity}")
    return milli // 1000


def kafka_lag(output):
    """Describe --offsets for the fresh, uncompacted topic with no expired records."""
    lag_by_partition = {}
    for line in output.splitlines():
        fields = line.split()
        if fields[:2] != [GROUP, TOPIC]:
            continue
        partition, committed, end, lag = fields[2:6]
        partition = int(partition)
        if partition in lag_by_partition or end == "-":
            raise ValueError("Kafka did not return one log end per partition")
        # Before the first commit, earliest on this fresh topic means offset zero.
        measured = int(end) - (0 if committed == "-" else int(committed))
        if measured < 0 or (lag != "-" and int(lag) != measured):
            raise ValueError("Kafka offsets and reported lag disagree")
        lag_by_partition[partition] = measured
    if set(lag_by_partition) != set(range(PARTITIONS)):
        raise ValueError("Missing partition observations for the lab consumer group")
    return sum(lag_by_partition.values())


def condition(obj, name):
    return next((c for c in obj.get("status", {}).get("conditions", []) if c["type"] == name), {})


def observed_pods(deployment, items):
    def controller_in(obj, uids):
        return any(ref.get("controller") and ref["uid"] in uids
                   for ref in obj["metadata"].get("ownerReferences", []))

    replicas = {item["metadata"]["uid"] for item in items if item["kind"] == "ReplicaSet"
                and controller_in(item, {deployment["metadata"]["uid"]})}
    result = []
    for pod in items:
        if (pod["kind"] != "Pod" or not controller_in(pod, replicas)
                or pod["metadata"].get("deletionTimestamp")):
            continue
        scheduled = condition(pod, "PodScheduled")
        result.append({
            "name": pod["metadata"]["name"],
            "phase": pod["status"].get("phase", "Unknown"),
            "ready": condition(pod, "Ready").get("status") == "True",
            "scheduled": {"True": True, "False": False}.get(scheduled.get("status")),
            "reason": scheduled.get("reason") or None,
            "message": scheduled.get("message") or None,
        })
    return sorted(result, key=lambda pod: pod["name"])


def scaling_observations(scaled, hpa, deployment, observed_at):
    spec = scaled["spec"]
    trigger, = spec["triggers"]
    config = trigger["metadata"]
    metric, = hpa["spec"]["metrics"]
    external = metric["external"]
    names = scaled["status"]
    if (metric["type"] != "External" or external["target"]["type"] != "AverageValue"
            or trigger["type"] != "kafka" or config["bootstrapServers"] != BROKER
            or config["topic"] != TOPIC or config["consumerGroup"] != GROUP
            or spec["scaleTargetRef"]["name"] != WORKER
            or hpa["spec"]["scaleTargetRef"]["name"] != WORKER
            or hpa["metadata"]["name"] != names["hpaName"]
            or not any(ref.get("controller") and ref["uid"] == scaled["metadata"]["uid"]
                       for ref in hpa["metadata"].get("ownerReferences", []))
            or [external["metric"]["name"]] != names["externalMetricNames"]
            or external["metric"]["selector"]["matchLabels"].get("scaledobject.keda.sh/name") != SCALED_OBJECT
            or hpa["spec"]["scaleTargetRef"]["kind"] != "Deployment"
            or any(obj["metadata"]["namespace"] != NAMESPACE for obj in (scaled, hpa, deployment))
            or scaled["metadata"]["name"] != SCALED_OBJECT
            or deployment["metadata"]["name"] != WORKER):
        raise ValueError("Autoscaler observations do not match the lab worker and Kafka scope")
    name = external["metric"]["name"]
    threshold = int(config["lagThreshold"])
    average_value = whole_units(external["target"]["averageValue"])
    if threshold <= 0 or threshold != average_value:
        raise ValueError("HPA average target does not match the Kafka lag threshold")
    return {
        "scaled_object": {
            "observed_at": observed_at, "target": TARGET, "name": scaled["metadata"]["name"],
            "hpa": names["hpaName"], "metric": name, "kafka_cluster": "failure-lab-kafka",
            "topic": config["topic"], "consumer_group": config["consumerGroup"],
            "lag_threshold": threshold, "scaler_profile": "keda-2.20.0-kafka",
            "plain_lag_mode": not config.get("partitionLimitation") and all(
                config.get(flag, "false") == "false" for flag in (
                    "excludePersistentLag", "limitToPartitionsWithLag",
                    "ensureEvenDistributionOfPartitions", "scaleToZeroOnInvalidOffset")),
            "allow_idle_consumers": config.get("allowIdleConsumers", "false") == "true",
        },
        "hpa": {
            "observed_at": observed_at, "target": TARGET, "name": hpa["metadata"]["name"],
            "metric": name, "target_average_value": average_value,
            "min_replicas": hpa["spec"]["minReplicas"], "max_replicas": hpa["spec"]["maxReplicas"],
            "desired_replicas": hpa["status"]["desiredReplicas"],
            "scaling_limited": {"True": True, "False": False}.get(
                condition(hpa, "ScalingLimited").get("status")),
        },
        "deployment": {
            "observed_at": observed_at, "target": TARGET, "desired_replicas": deployment["spec"]["replicas"],
            "ready_replicas": deployment["status"].get("readyReplicas", 0),
        },
    }


def scheduler_message_for_replay(message):
    # Kubernetes 1.34 can append a Dynamic Resource Allocation post-filter
    # note before preemption. It is not a second fit failure. Strip only
    # this exact suffix from an otherwise CPU-only scheduling decision.
    if message is not None:
        primary, separator, _ = message.partition(POST_FILTER_DRA)
        if separator and CPU_ONLY.fullmatch(primary):
            return primary.rstrip(".") + "."
    return message


def confirms_cpu_block(observations):
    hpa = observations["hpa"]
    deployment = observations["deployment"]
    pods = observations["pods"]["items"]
    desired = hpa["desired_replicas"]
    ready = [p for p in pods if p["ready"]]
    pending = [p for p in pods if not p["ready"]]
    return (desired == deployment["desired_replicas"] == len(pods)
            and len(ready) == deployment["ready_replicas"] == 1
            and bool(pending)
            and all(p["phase"] == "Running" and p["scheduled"] is True for p in ready)
            and all(p["phase"] == "Pending" and p["scheduled"] is False
                    and p["reason"] == "Unschedulable"
                    and CPU_ONLY.fullmatch((scheduler_message_for_replay(p["message"]) or "")
                                           .partition(". preemption:")[0])
                    for p in pending))


def verify_tickets(accepted, ledger):
    """Compare IDs, never just counts or Kafka offsets. Ledger is one SQL snapshot."""
    def duplicate_ids(ids):
        return sorted(key for key, count in Counter(ids).items() if count > 1)

    expected = set(accepted)
    reservations = {row["id"]: row for row in ledger["reservations"]}
    tickets = ledger["tickets"]
    counts = Counter(row["reservation_id"] for row in tickets)
    problems = {
        "duplicate_accepted_ids": duplicate_ids(accepted),
        "missing_reservations": sorted(expected - reservations.keys()),
        "unexpected_reservations": sorted(reservations.keys() - expected),
        "duplicate_reservations": duplicate_ids(row["id"] for row in ledger["reservations"]),
        "not_issued": sorted(key for key in expected & reservations.keys()
                             if reservations[key]["status"] != "ISSUED"),
        "missing_tickets": sorted(expected - counts.keys()),
        "duplicate_tickets": sorted(key for key, count in counts.items() if count > 1),
        "duplicate_ticket_ids": duplicate_ids(row["id"] for row in tickets),
        "unexpected_tickets": sorted(row["id"] for row in tickets if row["reservation_id"] not in expected),
        "unmatched_source_events": sorted(row["id"] for row in tickets
                                          if not row["source_event_id"]
                                          or row["source_event_id"] != reservations.get(
                                              row["reservation_id"], {}).get("event_id")),
    }
    return {
        "passed": bool(expected) and not any(problems.values()),
        "accepted": len(accepted), "reservations": len(reservations), "tickets": len(tickets),
        **problems,
    }


def main():
    parser = argparse.ArgumentParser(description="Recompute ticket verification from a saved lab report or kind run.")
    parser.add_argument("run", type=Path)
    args = parser.parse_args()
    run = json.loads(args.run.read_text(encoding="utf-8"))
    if run.get("format") == "failure-lab/incident-report-v1":
        run = run["verification"]
    elif run.get("format") != "failure-lab/kind-cpu-run-v1":
        parser.error("expected failure-lab/incident-report-v1 or failure-lab/kind-cpu-run-v1")
    result = verify_tickets(run["accepted_ids"], run["ledger"])
    print(json.dumps(result, indent=2))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
