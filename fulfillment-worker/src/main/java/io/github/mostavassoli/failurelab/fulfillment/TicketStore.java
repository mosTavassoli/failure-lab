package io.github.mostavassoli.failurelab.fulfillment;

import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

@ApplicationScoped
class TicketStore {
    private final DataSource dataSource;

    TicketStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Transactional
    boolean issue(ReservationAccepted event) {
        try (var connection = dataSource.getConnection()) {
            try (var check = connection.prepareStatement("""
                    SELECT 1 FROM reservation r JOIN outbox o ON o.reservation_id = r.id
                    WHERE r.id = ? AND r.sale_id = ? AND o.event_id = ? AND o.event_type = ?
                    """)) {
                check.setObject(1, event.reservationId());
                check.setObject(2, event.saleId());
                check.setObject(3, event.eventId());
                check.setString(4, ReservationAccepted.TYPE);
                try (var rows = check.executeQuery()) {
                    if (!rows.next()) {
                        throw new IllegalArgumentException("Event does not match an accepted reservation");
                    }
                }
            }

            boolean created;
            try (var insert = connection.prepareStatement("""
                    INSERT INTO ticket (id, reservation_id, source_event_id) VALUES (?, ?, ?)
                    ON CONFLICT (reservation_id) DO NOTHING
                    """)) {
                insert.setObject(1, UUID.randomUUID());
                insert.setObject(2, event.reservationId());
                insert.setObject(3, event.eventId());
                created = insert.executeUpdate() == 1;
            }
            if (created) {
                try (var update = connection.prepareStatement("UPDATE reservation SET status = 'ISSUED' WHERE id = ?")) {
                    update.setObject(1, event.reservationId());
                    update.executeUpdate();
                }
            }
            return created;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not issue ticket", e);
        }
    }
}
