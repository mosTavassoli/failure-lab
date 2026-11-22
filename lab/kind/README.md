# Run the Kubernetes CPU scheduling drill

The same Java API and worker run on a new single-node kind cluster with
PostgreSQL, four Kafka partitions, and KEDA's Kafka scaler. The drill creates
a healthy reservation first, then reserves enough node CPU to leave room
for the running worker but not for an additional one. The CPU reservation
is an idle pod with a resource **request**, not a program spinning on a CPU.

## Reproduce locally

Install Java 21, Maven, Python 3.11+, kind **0.31.0**, kubectl (1.34 or
compatible), and a local Docker engine with Linux containers. Make at
least 4 CPUs and 6 GB of RAM available to Docker, and leave at least 5 GiB
free on Docker's disk and the host temp drive. The first run needs internet
access for images and the pinned, checksum-verified KEDA **2.20.0** manifest.

From the Failure Lab root, with a new output directory each time:

```sh
mvn -pl reservation-api,fulfillment-worker -am -DskipTests package
python -B lab/kind/cpu_drill.py --out lab-runs/kind-cpu-01
```

On Windows, `py -B` also works. Use `--kind PATH` if kind is not on `PATH`.
The script creates and deletes its own randomly named cluster using a
temporary kubeconfig. It does not use the current Kubernetes context.
It loads two freshly built local images, does not push them, and exposes
the API to the drill only through a loopback port forward. No model or API
key is needed. If the process was forcibly interrupted, use the cluster
name printed at startup to clean up that cluster only. Pass a separate
lab kubeconfig path, not your default kubeconfig:

```sh
kind delete cluster --name failure-lab-cpu-RUN_SUFFIX --kubeconfig ./cleanup-lab-kubeconfig
```

## Read the output

- `incident.snapshot.json`: observations taken during the scheduling
  block, in `kafka-oncall/snapshot-v1` format. This file alone does not
  certify recovery. Kubernetes 1.34 can append a Dynamic Resource Allocation
  post-filter note before preemption; the exporter omits that note from the
  CPU-only decision for compatibility and keeps the original `PodScheduled`
  messages in `run.json`.
- `run.json`: timeline from a working reservation through the fault and
  recovery, the IDs accepted by HTTP, the PostgreSQL ledger, and the
  deterministic verification result. The drill fails if any accepted
  reservation doesn't have exactly one matching ticket or if unexpected
  tickets exist, even if consumer lag is zero.
- `incident-report.json`: the [shared replay format](../reports/README.md),
  with a sourced timeline, the blocked pod conditions, and the independently
  checkable ticket verdict. This does not replace either output above.

Recompute ticket verification from the captured ledger without starting a
cluster:

```sh
python -B lab/kind/evidence.py lab-runs/kind-cpu-01/run.json
```

## Replay the recorded incident

The [recorded snapshot](recorded/cpu-scheduling.snapshot.json) contains
normalized observations from the live drill. With Python 3.11+ and the two
repos checked out next to each other, run this from the
[Kafka On-Call](https://github.com/mosTavassoli/kafka-oncall) root:

```sh
python -B -m kafka_oncall replay ../failure-lab/lab/kind/recorded/cpu-scheduling.snapshot.json
```

The expected diagnosis is `cpu_scheduling`. No Docker, cluster, model, or
API key is needed. To replay your own run, pass the path to its
`incident.snapshot.json` instead. See the
[investigation runbook](../../knowledge/keda-cpu-scheduling.md).

This is a controlled scheduling failure, not a capacity benchmark. The
single node, four partitions, and per-event delay make the fault observable
with a small burst. Kafka and Kubernetes observations are near-time samples,
not an atomic cross-system snapshot; the ticket ledger is read in one SQL
statement after the group drains.

## Verified local run

On November 21, 2026, Kubernetes 1.34.3 and KEDA 2.20.0 reproduced the block
against the live ticket workload:

| Phase | Observed result |
| --- | --- |
| Baseline | 1 of 1 worker ready; the first reservation issued exactly one ticket. |
| Blocked | HPA requested 4 workers, but only 1 was ready. All 3 pending pods reported `Insufficient cpu`: 125 mCPU remained and each worker requested 250 mCPU. |
| Backlog | Kafka lag was 60 messages. KEDA exposed 8, consistent with four partitions times the lag target of two. That cap did not explain the three already-requested pods that failed to schedule. |
| Recovered | All 4 workers became ready and lag fell to zero. PostgreSQL had exactly one matching ticket for each of 65 accepted reservation IDs, with none missing, duplicated, or unexpected. |
| Offline replay | Kafka On-Call returned `cpu_scheduling`, citing the ScaledObject, HPA, Deployment, and pod observations. |

The normalized scheduling snapshot and an [aliased replay report](../reports/recorded/kind-cpu-scheduling.report.json)
from the same run are checked in. Generated run directories, including the
ledger with original synthetic IDs, are git-ignored. The cluster was deleted
after the run.
