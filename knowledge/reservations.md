# Reservations: how to read the response

A successful reservation consumes one ticket from a sale and records an
acceptance event. `PENDING` means the reservation was committed, not that
a ticket was issued. In the current slice there is no publisher or worker,
so `PENDING` does not yet indicate an incident.

| Signal | Meaning | Next check |
| --- | --- | --- |
| `201` | New reservation was accepted. | Follow `Location` for current status. |
| `200` on a retry | The original reservation exists. | Check that it has the same reservation ID and sale. |
| `409 SOLD_OUT` | No capacity remained when the reservation update ran. | `GET /api/sales/{id}` shows the current inventory. |
| `409 IDEMPOTENCY_CONFLICT` | The key already identifies a reservation for a different sale. | Verify the original intent; use a new key only for a new intent. |

If the client timed out, it cannot tell whether the server committed.
Retry the same sale with the **same** idempotency key. A new key would
express a new reservation intent even if the first request succeeded.
An error from the outbox insert should leave neither a reservation nor a
stock decrement; the integration suite checks this against PostgreSQL.
