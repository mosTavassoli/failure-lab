import json
import unittest
from pathlib import Path

from evidence import (
    BROKER, GROUP, NAMESPACE, SCALED_OBJECT, TOPIC, WORKER,
    confirms_cpu_block, kafka_lag, observed_pods, scaling_observations, scheduler_message_for_replay,
    verify_tickets, whole_units,
)


class KafkaLagTest(unittest.TestCase):
    def test_only_the_scoped_group_and_topic_and_accounts_for_uncommitted_offsets(self):
        self.assertEqual(12, kafka_lag(f"""
            GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
            {GROUP} {TOPIC} 0 3 7 4
            {GROUP} {TOPIC} 1 - 8 -
            {GROUP} {TOPIC} 2 9 9 0
            {GROUP} {TOPIC} 3 - 0 -
            other-group {TOPIC} 0 0 99 99
            {GROUP} other-topic 0 0 99 99
        """))

    def test_missing_partition_cannot_be_reported_as_zero(self):
        with self.assertRaisesRegex(ValueError, "Missing partition"):
            kafka_lag(f"{GROUP} {TOPIC} 0 3 3 0")

    def test_metric_quantities_are_not_rounded(self):
        self.assertEqual(8, whole_units("8000m"))
        self.assertEqual(8, whole_units("8"))
        with self.assertRaises(ValueError):
            whole_units("800m")


class SchedulingEvidenceTest(unittest.TestCase):
    def test_recorded_snapshot_preserves_the_live_cpu_block(self):
        path = Path(__file__).with_name("recorded") / "cpu-scheduling.snapshot.json"
        snapshot = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual("kafka-oncall/snapshot-v1", snapshot["format"])
        self.assertTrue(confirms_cpu_block(snapshot["observations"]))

    def test_normalizes_keda_and_hpa_only_when_the_scope_matches(self):
        target = {"kind": "Deployment", "name": WORKER}
        metric = "s0-kafka-reservation-accepted-v1"
        scaled = {
            "metadata": {"namespace": NAMESPACE, "name": SCALED_OBJECT, "uid": "scaler"},
            "spec": {"scaleTargetRef": target, "triggers": [{"type": "kafka", "metadata": {
                "bootstrapServers": BROKER, "topic": TOPIC, "consumerGroup": GROUP, "lagThreshold": "2",
            }}]},
            "status": {"hpaName": "keda-hpa-fulfillment", "externalMetricNames": [metric]},
        }
        hpa = {
            "metadata": {"namespace": NAMESPACE, "name": "keda-hpa-fulfillment",
                         "ownerReferences": [{"controller": True, "uid": "scaler"}]},
            "spec": {"scaleTargetRef": target, "minReplicas": 1, "maxReplicas": 4,
                     "metrics": [{"type": "External", "external": {
                         "metric": {"name": metric, "selector": {
                             "matchLabels": {"scaledobject.keda.sh/name": SCALED_OBJECT},
                         }},
                         "target": {"type": "AverageValue", "averageValue": "2000m"},
                     }}]},
            "status": {"desiredReplicas": 4, "conditions": [{"type": "ScalingLimited", "status": "False"}]},
        }
        deployment = {"metadata": {"namespace": NAMESPACE, "name": WORKER},
                      "spec": {"replicas": 4}, "status": {"readyReplicas": 1}}
        observed_at = "2026-01-01T00:00:00Z"
        obs = scaling_observations(scaled, hpa, deployment, observed_at)
        self.assertEqual(GROUP, obs["scaled_object"]["consumer_group"])
        self.assertEqual(2, obs["hpa"]["target_average_value"])
        self.assertEqual(4, obs["hpa"]["desired_replicas"])
        self.assertEqual(1, obs["deployment"]["ready_replicas"])

        scaled["spec"]["triggers"][0]["metadata"]["consumerGroup"] = "different-group"
        with self.assertRaisesRegex(ValueError, "scope"):
            scaling_observations(scaled, hpa, deployment, observed_at)

    def test_uses_controller_ownership_not_only_the_selector(self):
        def metadata(uid, name, controller=None):
            return {"uid": uid, "name": name, "ownerReferences": (
                [{"uid": controller, "controller": True}] if controller else []
            )}

        deployment = {"metadata": metadata("deployment", "fulfillment-worker")}
        items = [
            {"kind": "ReplicaSet", "metadata": metadata("our-rs", "worker-rs", "deployment")},
            {"kind": "ReplicaSet", "metadata": metadata("foreign-rs", "not-ours", "another-deployment")},
            {"kind": "Pod", "metadata": metadata("a", "worker-one", "our-rs"), "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}, {"type": "PodScheduled", "status": "True"}],
            }},
            {"kind": "Pod", "metadata": metadata("b", "unrelated-pod", "foreign-rs"), "status": {}},
        ]
        self.assertEqual([{"name": "worker-one", "phase": "Running", "ready": True, "scheduled": True,
                           "reason": None, "message": None}], observed_pods(deployment, items))

    def test_a_mixed_or_incomplete_scheduling_failure_is_not_proven_cpu_only(self):
        ready = {"phase": "Running", "ready": True, "scheduled": True}
        pending = {"phase": "Pending", "ready": False, "scheduled": False, "reason": "Unschedulable",
                   "message": "0/1 nodes are available: 1 Insufficient cpu. preemption: no victims."}
        obs = {
            "hpa": {"desired_replicas": 2},
            "deployment": {"desired_replicas": 2, "ready_replicas": 1},
            "pods": {"items": [ready, pending]},
        }
        self.assertTrue(confirms_cpu_block(obs))
        pending["message"] = "0/1 nodes are available: 1 Insufficient cpu, 1 Insufficient memory."
        self.assertFalse(confirms_cpu_block(obs))
        obs["pods"]["items"].pop()
        self.assertFalse(confirms_cpu_block(obs))

    def test_ignores_only_known_post_filter_commentary_in_kubernetes_1_34(self):
        note = (". no new claims to deallocate, preemption: "
                "0/1 nodes are available: 1 No preemption victims found for incoming pod.")
        cpu_only = "0/1 nodes are available: 1 Insufficient cpu"
        self.assertEqual(cpu_only + ".", scheduler_message_for_replay(cpu_only + note))
        mixed = cpu_only + ", 1 Insufficient memory" + note
        self.assertEqual(mixed, scheduler_message_for_replay(mixed))
        unknown = cpu_only + ". other finding, preemption: not available"
        self.assertEqual(unknown, scheduler_message_for_replay(unknown))


class TicketVerificationTest(unittest.TestCase):
    def test_requires_exactly_one_issued_ticket_and_matching_source_event_per_accepted_id(self):
        result = verify_tickets(["a", "b"], {
            "reservations": [{"id": key, "status": "ISSUED", "event_id": f"event-{key}"} for key in ("a", "b")],
            "tickets": [{"id": f"ticket-{key}", "reservation_id": key, "source_event_id": f"event-{key}"}
                        for key in ("b", "a")],
        })
        self.assertTrue(result["passed"])

    def test_duplicate_and_missing_tickets_fail_even_when_counts_match(self):
        result = verify_tickets(["a", "b"], {
            "reservations": [{"id": key, "status": "ISSUED", "event_id": f"event-{key}"} for key in ("a", "b")],
            "tickets": [{"id": key, "reservation_id": "a", "source_event_id": "event-a"} for key in ("t1", "t2")],
        })
        self.assertEqual(result["accepted"], result["tickets"])
        self.assertFalse(result["passed"])
        self.assertEqual(["b"], result["missing_tickets"])
        self.assertEqual(["a"], result["duplicate_tickets"])

    def test_unexpected_ticket_and_wrong_event_fail_even_when_counts_match(self):
        result = verify_tickets(["a", "b"], {
            "reservations": [{"id": key, "status": "ISSUED", "event_id": f"event-{key}"} for key in ("a", "b")],
            "tickets": [
                {"id": "t1", "reservation_id": "a", "source_event_id": "event-b"},
                {"id": "t2", "reservation_id": "not-accepted", "source_event_id": "event-other"},
            ],
        })
        self.assertFalse(result["passed"])
        self.assertEqual(["t2"], result["unexpected_tickets"])
        self.assertEqual(["t1", "t2"], result["unmatched_source_events"])
