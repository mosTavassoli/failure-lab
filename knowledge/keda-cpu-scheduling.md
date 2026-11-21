# Recovery drill: KEDA requests workers Kubernetes cannot schedule

**Question:** Is the autoscaler failing to request more workers, or did it
request the workers and the scheduler could not place them?

## Follow the evidence

1. Check topic and consumer group scope. The lab feeds the real reservation
   API 64 synthetic requests; the outbox, not the drill, publishes the
   events. A lab-only bounded processing delay makes the backlog observable
   without requiring a large load generator.
2. Read the ScaledObject and its HPA. The HPA's **desired** count answers
   whether it requested more replicas. A Kafka partition cap or an HPA
   replica limit can bound that count, but neither explains pods already
   requested and unable to schedule.
3. Compare the Deployment's desired and **ready** counts, then inspect only
   pods owned by that Deployment's ReplicaSets. `Pending` alone is not
   enough: check `PodScheduled=False`, `Unschedulable`, and `Insufficient
   cpu` for all blocked replicas. Other scheduler reasons or a rollout in
   progress need separate investigation.
4. Check schedulable headroom (allocatable CPU minus CPU **requests**);
   instantaneous usage can be low. In the drill, removing the idle
   reservation pod restores headroom. Do not copy this remediation to a
   real cluster without checking the workload and placement constraints.

## Verify recovery

Watch the requested worker pods become ready and the consumer backlog
drain. Then reconcile the IDs returned by the reservation API against a
PostgreSQL ledger read: exactly one `ISSUED` reservation and one ticket
backed by its acceptance event per accepted ID, with no extra tickets.
Lag is transport evidence, not business verification.

The [reproduction guide](../lab/kind/README.md) explains how to run the
drill and replay the snapshot with Kafka On-Call.
