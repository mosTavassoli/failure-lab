# Failure Lab

**Break a running system, investigate what happened, and prove whether it recovered.**
The first system under test is a ticket reservation and fulfillment app.
New incidents will share an evidence timeline, runbook links, and recovery
reports—not a separate app for each lesson.

## Status

The reservation API is the first working slice: Java 21, Quarkus, and
PostgreSQL. It accepts one ticket per request, prevents overselling, and
commits the reservation and its outbox event together. A unit of capacity
is consumed only once for a successful idempotency key.

Events are stored, **not yet published**. The API returns `PENDING`; there
is no fulfillment worker, Kubernetes lab, web UI, or agent in this slice.
See the [design and roadmap](docs/design.md) and the [first Knowledge
page](knowledge/reservations.md).

## Run locally

Requirements: **Java 21, Maven, Docker with Linux containers**. Quarkus Dev
Services starts a disposable PostgreSQL container. Run from the repo root:

```sh
mvn verify
mvn -f reservation-api/pom.xml quarkus:dev
```

Use these requests from a REST client. The development profile seeds a sale
with 12 tickets and binds to `127.0.0.1`:

```http
GET http://127.0.0.1:8080/api/sales/f0000000-0000-4000-8000-000000000001
```

```http
POST http://127.0.0.1:8080/api/reservations
Content-Type: application/json
Idempotency-Key: f0000000-0000-4000-8000-000000000002

{"saleId":"f0000000-0000-4000-8000-000000000001"}
```

The first successful request returns `201` and a `Location` for the
reservation. Sending the same sale and key again returns `200` with the
same reservation. Use a new key only for a new reservation intent. Sales
with no available tickets return `409 SOLD_OUT`.

This is a local lab for synthetic data, not a public checkout. There are
no real payments, user accounts, or production integrations.
