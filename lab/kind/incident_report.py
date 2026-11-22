"""Project a captured CPU drill into the shared, self-contained replay report."""

from datetime import datetime

from evidence import confirms_cpu_block, verify_tickets

STAGES = {
    "baseline": ("The baseline reservation issued with one worker ready.",
                 ("Reservation API", "Kubernetes HPA and Deployment", "Kafka consumer group", "PostgreSQL"),
                 ("hpa_desired", "ready", "lag", "accepted", "tickets")),
    "cpu_reserved": ("An idle pod reserved schedulable CPU.",
                     ("Kubernetes node and pod requests",),
                     ("allocatable_mcpu", "previously_requested_mcpu", "reserved_mcpu",
                      "worker_request_mcpu", "remaining_mcpu")),
    "burst_accepted": ("The API accepted the burst of new reservations.",
                       ("Reservation API (HTTP 201)",), ("count",)),
    "cpu_blocked": ("The HPA requested more workers, but the scheduler reported insufficient CPU.",
                    ("Kubernetes HPA and Deployment", "PodScheduled conditions",
                     "Kafka consumer group", "KEDA external metrics"),
                    ("hpa_desired", "ready", "lag", "external_metric")),
    "cpu_released": ("The drill removed the idle CPU reservation pod.",
                     ("Kubernetes API",), ()),
    "workers_ready": ("The requested workers became ready.",
                      ("Kubernetes Deployment",), ("ready",)),
    "lag_drained": ("The worker consumer group reached zero lag.",
                    ("Kafka consumer group",), ("lag",)),
    "tickets_reconciled": ("Checked accepted reservation IDs against the PostgreSQL ledger.",
                           ("Reservation API", "PostgreSQL"), ()),
}


def cpu_incident_report(run, snapshot):
    if (run["format"] != "failure-lab/kind-cpu-run-v1" or snapshot["format"] != "kafka-oncall/snapshot-v1"
            or run["scenario"] != snapshot["id"] or run["target"] != snapshot["target"]):
        raise ValueError("Expected a matching CPU run and scheduling snapshot")

    captured = run["timeline"]
    blocked = next(event for event in captured if event["stage"] == "cpu_blocked")
    # A run from another attempt can have the same scenario and target.
    obs = snapshot["observations"]
    pods = obs["pods"]
    pending = [pod for pod in pods["items"] if pod["scheduled"] is False]
    capture_age = datetime.fromisoformat(blocked["observed_at"]) - datetime.fromisoformat(snapshot["as_of"])
    if (not confirms_cpu_block(obs)
            or not 0 <= capture_age.total_seconds() <= 30
            or blocked["scheduler_messages"].keys() != {pod["name"] for pod in pending}
            or blocked["hpa_desired"] != obs["hpa"]["desired_replicas"]
            or blocked["ready"] != obs["deployment"]["ready_replicas"]
            or blocked["lag"] != obs["kafka"]["lag"]
            or blocked["external_metric"] != obs["external_metric"]["value"]):
        raise ValueError("The scheduling snapshot does not match the CPU block in the run")

    accepted = run["accepted_ids"]
    # Only the synthetic IDs, status and event links are needed to check recovery offline.
    ledger = {
        "reservations": [{field: row[field] for field in ("id", "status", "event_id")}
                         for row in run["ledger"]["reservations"]],
        "tickets": [{field: row[field] for field in ("id", "reservation_id", "source_event_id")}
                    for row in run["ledger"]["tickets"]],
    }
    result = verify_tickets(accepted, ledger)
    timeline = []
    for event in captured:
        stage = event["stage"]
        summary, sources, fields = STAGES[stage]
        facts = {field: event[field] for field in fields}
        if stage == "cpu_blocked":
            facts["pod_observed_at"] = pods["observed_at"]
            facts["blocked_pods"] = [{field: pod[field] for field in ("name", "reason", "message")}
                                     for pod in pending]
        elif stage == "tickets_reconciled":
            facts = {field: result[field] for field in ("accepted", "tickets", "passed")}
        timeline.append({"stage": stage, "observed_at": event["observed_at"], "sources": list(sources),
                         "summary": summary, "facts": facts})

    return {
        "format": "failure-lab/incident-report-v1",
        "scenario": run["scenario"],
        "title": "KEDA scale-out blocked by CPU requests",
        "target": run["target"],
        "runbook": "knowledge/keda-cpu-scheduling.md",
        "timeline": timeline,
        "diagnosis": {
            "summary": "Autoscaling requested more workers. Existing CPU requests left too little room to schedule them.",
            "evidence_stages": ["cpu_reserved", "cpu_blocked"],
        },
        "verification": {"accepted_ids": accepted, "ledger": ledger, "result": result},
    }
