import copy
import json
import unittest

from jsonschema import Draft202012Validator, ValidationError

from lab.kind.evidence import verify_tickets
from lab.reports.report import HERE, SCHEMA, check_report, for_publication


def recorded(scenario):
    return json.loads((HERE / "recorded" / f"{scenario}.report.json").read_text(encoding="utf-8"))


class IncidentReportTest(unittest.TestCase):
    def test_both_recorded_drills_conform_to_the_same_schema_and_reconcile(self):
        Draft202012Validator.check_schema(SCHEMA)
        for scenario, count in (("worker-redelivery", 2), ("kind-cpu-scheduling", 65)):
            with self.subTest(scenario=scenario):
                report = recorded(scenario)
                self.assertEqual(scenario, report["scenario"])
                result = check_report(report)
                self.assertTrue(result["passed"])
                self.assertEqual((count, count), (result["accepted"], result["tickets"]))

    def test_replay_redelivery_retains_the_same_ticket_and_commits_past_both_offsets(self):
        report = recorded("worker-redelivery")
        stages = {event["stage"]: event["facts"] for event in report["timeline"]}
        held = stages["ack_held"]
        self.assertEqual(held["ticket_id"], stages["redelivered"]["ticket_id"])
        self.assertEqual(held["reservation_id"], stages["redelivered"]["reservation_id"])
        self.assertEqual(stages["event_published"]["event_id"], stages["redelivered"]["event_id"])
        self.assertEqual("PENDING", stages["second_pending"]["status"])
        final_ticket, = (row for row in report["verification"]["ledger"]["tickets"]
                         if row["reservation_id"] == held["reservation_id"])
        self.assertEqual(held["ticket_id"], final_ticket["id"])
        for stage in ("ack_held", "worker_crashed"):
            committed = stages[stage]["committed_next_offset"]
            self.assertTrue(committed is None or committed <= stages[stage]["event_offset"])
        self.assertEqual(held["event_offset"] + 2, stages["offsets_committed"]["next_offset"])

    def test_cpu_report_separates_the_scheduling_block_from_reconciliation(self):
        report = recorded("kind-cpu-scheduling")
        stages = {event["stage"]: event["facts"] for event in report["timeline"]}
        blocked = stages["cpu_blocked"]
        self.assertEqual((4, 1), (blocked["hpa_desired"], blocked["ready"]))
        self.assertEqual(3, len(blocked["blocked_pods"]))
        for pod in blocked["blocked_pods"]:
            self.assertEqual("Unschedulable", pod["reason"])
            self.assertIn("Insufficient cpu", pod["message"])
        self.assertEqual(4, stages["workers_ready"]["ready"])
        self.assertEqual(0, stages["lag_drained"]["lag"])
        self.assertNotIn("passed", stages["lag_drained"])

    def test_same_count_with_a_missing_and_duplicate_ticket_is_not_recovery(self):
        report = recorded("kind-cpu-scheduling")
        verification = report["verification"]
        first, second, *_ = verification["ledger"]["tickets"]
        missing = second["reservation_id"]
        second.update(reservation_id=first["reservation_id"], source_event_id=first["source_event_id"])
        with self.assertRaisesRegex(ValueError, "Stored verification does not match"):
            check_report(report)  # A stale green verdict cannot pass the contract check.

        result = verify_tickets(verification["accepted_ids"], verification["ledger"])
        self.assertEqual(result["accepted"], result["tickets"])
        self.assertFalse(result["passed"])
        self.assertEqual([missing], result["missing_tickets"])
        self.assertEqual([first["reservation_id"]], result["duplicate_tickets"])
        verification["result"] = result
        report["timeline"][-1]["facts"]["passed"] = False
        self.assertFalse(check_report(report)["passed"])  # A truthful failing report is structurally valid.

    def test_publication_aliases_ids_without_changing_evidence_or_verdict(self):
        raw = "f0000000-0000-4000-8000-000000000001"
        report = json.loads(json.dumps(recorded("worker-redelivery")).replace("reservation-001", raw))
        original = copy.deepcopy(report)
        public = for_publication(report)
        self.assertEqual(report, original)
        self.assertEqual(report["diagnosis"], public["diagnosis"])
        self.assertEqual(report["verification"]["result"], public["verification"]["result"])
        first = public["verification"]["accepted_ids"][0]
        self.assertEqual("reservation-001", first)
        held = next(event["facts"] for event in public["timeline"] if event["stage"] == "ack_held")
        self.assertEqual(first, held["reservation_id"])
        self.assertRegex(held["ticket_id"], r"^ticket-\d{3}$")
        self.assertNotIn(raw, json.dumps(public))
        self.assertTrue(check_report(public)["passed"])

    def test_requires_evidence_sources_and_resolvable_stage_references(self):
        report = recorded("kind-cpu-scheduling")
        report["diagnosis"]["evidence_stages"] = ["not_captured"]
        with self.assertRaisesRegex(ValueError, "Diagnosis cites a stage not present"):
            check_report(report)
        del report["timeline"][0]["sources"]
        with self.assertRaises(ValidationError):
            check_report(report)
