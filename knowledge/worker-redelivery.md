# Recovery drill: ticket committed, event not acknowledged

**Question:** If a worker dies after issuing a ticket but before acknowledging
its Kafka event, does the next worker issue it again? Can it still process
new reservations?

Run `mvn verify` from the repository root with Docker running. The
[`RedeliveryIT`](../recovery-it/src/test/java/io/github/mostavassoli/failurelab/recovery/RedeliveryIT.java)
integration test starts disposable PostgreSQL and Kafka containers and launches
the packaged API and worker as separate JVM processes.

1. Accept the first reservation and wait for the relay to publish its event.
2. Start a worker with a one-reservation lab hold on acknowledgement. Wait
   for the committed ticket and verify that the consumer group has not
   committed past this event.
3. Force that lab process to exit and accept a second reservation while
   there is no worker. It remains `PENDING`.
4. Start the worker without the hold. Verify it observes the first event
   again, preserves the original ticket ID, issues the second ticket, and
   commits past both offsets.
5. Reconcile the accepted reservation IDs against the issued tickets in
    PostgreSQL. Missing or duplicate tickets fail the test even if Kafka
    has drained.

The test writes `recovery-it/target/incident-report.json`. The
[recorded report](../lab/reports/recorded/worker-redelivery.report.json) shows
the held offset, the preserved ticket ID, and the final ledger using synthetic
aliases. It can be read and [checked offline](../lab/reports/README.md#verify-offline).

The hold requires explicit lab mode and an exact reservation UUID. There is
no HTTP endpoint to crash a worker, and the test kills only a process it
started against its own containers. Use synthetic data. Kafka offset progress
is evidence of processing, not proof of business recovery.
