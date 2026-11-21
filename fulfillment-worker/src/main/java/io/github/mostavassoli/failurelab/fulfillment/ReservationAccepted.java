package io.github.mostavassoli.failurelab.fulfillment;

import java.time.OffsetDateTime;
import java.util.UUID;

public record ReservationAccepted(UUID eventId, UUID reservationId, UUID saleId,
                                  String type, OffsetDateTime occurredAt) {
    public static final String TYPE = "reservation.accepted.v1";

    public ReservationAccepted {
        if (eventId == null || reservationId == null || saleId == null || occurredAt == null || !TYPE.equals(type)) {
            throw new IllegalArgumentException("Invalid reservation acceptance");
        }
    }
}
