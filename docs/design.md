# Failure Lab design and roadmap

## Design goal

Show why a system failed and prove recovery against application state.
For ticketing, a drained Kafka backlog is not proof: every accepted
reservation must have exactly one ticket and there must be no tickets for
unaccepted reservations. The app is a workload for the lab, not a requirement
that every future incident be about ticketing.

| Milestone | Evidence of completion |
| --- | --- |
| **1. Reservation API** | Real PostgreSQL tests show last-ticket contention, idempotent retry, and rollback if the acceptance event cannot be saved. |
| **2. Fulfillment and redelivery** | The [redelivery drill](../knowledge/worker-redelivery.md) launches packaged apps against real Kafka and PostgreSQL. It stops the worker after commit and before acknowledgement, then checks redelivery, the original ticket ID, and reconciliation for both accepted orders. |
| **3. Kubernetes/KEDA incident** | Run the same workload on kind. Capture a CPU scheduling failure and replay those observations with [Kafka On-Call](https://github.com/mosTavassoli/kafka-oncall). |
| **4. Web and Knowledge** | Ship the React/TypeScript customer, incident, and Knowledge views. Both incidents must use the same evidence timeline and verification report. Record a public replay with no model or live cluster dependency. |
| **5. Operate and extend** | Add OpenTelemetry and Prometheus where the incidents need them, test Redis as a non-authoritative status cache, and plug in the read-only HolmesGPT skill and versioned runbooks. Add a PostgreSQL lock incident without replacing the report format. |

These are milestones, not separate platforms. Start the UI once the worker
can produce a real timeline. Add no infrastructure just to put it in the
stack list. Kafka On-Call, Streamjack, and Infra Agent Conformance remain
independent.

## Current transaction boundary

The API uses PostgreSQL at its default `READ COMMITTED` isolation. A
reservation command uses a single transaction:

1. Claim a client-supplied UUID request key with a unique constraint. If a
   committed reservation already owns it, return that reservation for
   the same sale or reject reuse for a different sale.
2. Decrement capacity with one conditional `UPDATE ... WHERE remaining > 0`.
   PostgreSQL rechecks the predicate after a contending transaction
   changes the row. There is no separate availability check followed by an
   unconditional write.
3. Insert an outbox row with an immutable `reservation.accepted.v1` payload
   (`eventId`, `reservationId`, `saleId`, `type`, `occurredAt`). Roll back the
   claim and inventory update on failure.

## Delivery and fulfillment

The scheduled relay claims an unpublished outbox row with
`FOR UPDATE SKIP LOCKED`, publishes with the reservation ID as the Kafka key,
and marks the row published only after broker acknowledgement. It holds a
row lock across the bounded broker request; this is a local-first trade-off
for a simple relay, not a high-throughput design. If the process stops
after broker acknowledgement but before its SQL commit, it can send the
same payload and `eventId` again. Kafka producer idempotence does not make
these two writes atomic.

The worker validates that the event identifies a durable acceptance, inserts
a ticket and marks the reservation issued in the same PostgreSQL transaction,
then acknowledges the Kafka delivery. A unique constraint on `ticket.reservation_id`
makes redelivery a no-op. This gives one durable ticket per reservation
without assuming exactly-once Kafka delivery. The lab uses one database shared
by the API and worker, and it retains outbox rows so the worker can validate
source events. Malformed events fail the consumer rather than being silently
committed; a dead-letter workflow and retention policy are out of scope.

## Knowledge boundaries

Markdown under [`knowledge/`](../knowledge/) is the first source for the
future Knowledge view. Runbooks describe the system and how to interpret
findings. The live or recorded observations establish what happened.
Future AI tools read only bounded, scoped evidence; recovery verification
stays deterministic and outside the model. Keep lab workloads disposable
and use synthetic data only.
