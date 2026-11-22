"""Check a saved incident report and optionally alias synthetic IDs for publication."""

import argparse
import json
from datetime import datetime
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError

from lab.kind.evidence import verify_tickets

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SCHEMA = json.loads((HERE / "incident-report-v1.schema.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA)


def check_report(report):
    """Check cross-field relationships that the schema alone cannot express."""
    VALIDATOR.validate(report)
    if not (ROOT / report["runbook"]).is_file():
        raise ValueError(f"Runbook does not exist: {report['runbook']}")
    timeline = report["timeline"]
    stages = [event["stage"] for event in timeline]
    times = [datetime.fromisoformat(event["observed_at"]) for event in timeline]
    if len(set(stages)) != len(stages) or times != sorted(times):
        raise ValueError("Timeline stages must be unique and ordered by observation time")
    if not set(report["diagnosis"]["evidence_stages"]).issubset(stages):
        raise ValueError("Diagnosis cites a stage not present in the timeline")

    verification = report["verification"]
    result = verify_tickets(verification["accepted_ids"], verification["ledger"])
    if result != verification["result"]:
        raise ValueError("Stored verification does not match the accepted IDs and ledger")
    if (stages[-1] != "tickets_reconciled"
            or timeline[-1]["facts"] != {key: result[key] for key in ("accepted", "tickets", "passed")}):
        raise ValueError("The last timeline entry must show the reconciliation result")
    return result


def for_publication(report):
    """Alias IDs consistently in the ledger and timeline of a synthetic report."""
    check_report(report)
    verification = report["verification"]
    reservations = verification["ledger"]["reservations"]
    tickets = verification["ledger"]["tickets"]
    aliases = {}

    def add_ids(prefix, ids):
        for index, value in enumerate(dict.fromkeys(value for value in ids if value is not None), 1):
            aliases.setdefault(value, f"{prefix}-{index:03d}")

    add_ids("reservation", [*verification["accepted_ids"], *(row["id"] for row in reservations),
                            *(row["reservation_id"] for row in tickets)])
    add_ids("ticket", [row["id"] for row in tickets])
    add_ids("event", [*(row["event_id"] for row in reservations), *(row["source_event_id"] for row in tickets)])

    def replace(value):
        if isinstance(value, dict):
            return {key: replace(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace(item) for item in value]
        return aliases.get(value, value) if isinstance(value, str) else value

    public = replace(report)
    verification = public["verification"]
    # Sort issue IDs in the new namespace as the offline verifier would.
    verification["result"] = verify_tickets(verification["accepted_ids"], verification["ledger"])
    check_report(public)
    return public


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", type=Path, nargs="+")
    parser.add_argument("--record-as", type=Path, help="write one aliased synthetic report to a new file")
    args = parser.parse_args()
    if args.record_as is not None and len(args.reports) != 1:
        parser.error("--record-as takes exactly one input report")

    passed = True
    try:
        for path in args.reports:
            report = json.loads(path.read_text(encoding="utf-8"))
            result = check_report(report)
            passed = passed and result["passed"]
            verdict = "passed" if result["passed"] else "failed"
            print(f"{path}: valid report; ticket verification {verdict} "
                  f"({result['accepted']} accepted, {result['tickets']} tickets)")
            if args.record_as is not None and result["passed"]:
                public = for_publication(report)
                with args.record_as.open("x", encoding="utf-8", newline="\n") as out:
                    out.write(json.dumps(public, indent=2, ensure_ascii=False) + "\n")
    except (OSError, ValidationError, ValueError) as exc:
        parser.error(str(exc))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
