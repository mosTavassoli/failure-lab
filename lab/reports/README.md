# Shared incident reports

Both drills emit [`failure-lab/incident-report-v1`](incident-report-v1.schema.json).
The recorded reports are inputs for the v1.0 replay, not scripted simulations
of what a successful run should look like:

- [Worker crash and Kafka redelivery](recorded/worker-redelivery.report.json):
  the worker preserved the original ticket and issued the second one.
- [KEDA CPU scheduling](recorded/kind-cpu-scheduling.report.json): four
  workers were requested, only one was ready, and all 65 accepted reservations
  eventually reconciled. This is the November 21 capture used for the
  [recorded scheduling snapshot](../kind/recorded/cpu-scheduling.snapshot.json).

## What a report contains

- **Timeline:** Observations in capture order. `observed_at` is the time the
  drill observed the fact, not necessarily when the underlying event occurred.
  Each entry names its sources. `facts` differ by scenario.
- **Diagnosis:** A short explanation referencing timeline stages. This is
  separate from the ticket verdict. Neither a healthy replica count nor a
  committed Kafka offset establishes that every order got a ticket.
- **Verification:** Reservation IDs from HTTP 201 responses and the matching
  PostgreSQL evidence, read using [one statement](../ticket-ledger.sql) in the
  disposable database. The result records missing, duplicate, or unexpected
  IDs and mismatches with the acceptance events.

The published reports replace synthetic reservation, ticket, and event UUIDs
with report-local aliases. The links between IDs, observations, and the verdict
are preserved. Raw logs, connection details, and unprojected run files are not
included. `runbook` is a repository-relative path to the matching Knowledge page.

## Verify offline

Recompute the ticket result without Docker, pip packages, or an API key:

```sh
python -B lab/kind/evidence.py lab/reports/recorded/kind-cpu-scheduling.report.json
```

For schema and consistency checks, use Python 3.11+ with the test dependency
installed. Run these commands from the repository root:

```sh
python -m pip install -r lab/reports/requirements-test.txt
python -B -m unittest discover -s lab/reports -v
python -B -m lab.reports.report recovery-it/target/incident-report.json
```

The last command checks the report produced by `mvn verify`, including that the
Java verdict matches independent offline recomputation. It exits nonzero for
an inconsistent report or a failed ticket check. The tests also show that an
unchanged total can hide both a missing and a duplicated ticket.

To prepare a new sample, pass `--record-as NEW_PATH` to the last command with
the output of a successful **synthetic** drill. Review the new file before
publishing it. The alias step is not a scrubber for arbitrary logs or data.
