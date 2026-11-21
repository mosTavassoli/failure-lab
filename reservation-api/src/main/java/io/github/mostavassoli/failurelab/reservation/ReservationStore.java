package io.github.mostavassoli.failurelab.reservation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class ReservationStore {
    private final DataSource dataSource;

    public ReservationStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public record Sale(UUID id, String name, int capacity, int remaining) {}

    public record Reservation(UUID id, UUID saleId, String status) {}

    public record Outcome(Reservation reservation, boolean created) {}

    enum Rejection {
        SALE_NOT_FOUND, SOLD_OUT, IDEMPOTENCY_CONFLICT
    }

    static final class Rejected extends RuntimeException {
        final Rejection reason;

        Rejected(Rejection reason) {
            super(reason.name());
            this.reason = reason;
        }
    }

    public Optional<Sale> findSale(UUID id) {
        try (var connection = dataSource.getConnection();
             var query = connection.prepareStatement("SELECT * FROM sale WHERE id = ?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                return rows.next() ? Optional.of(new Sale(id, rows.getString("name"),
                        rows.getInt("capacity"), rows.getInt("remaining"))) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not read sale", e);
        }
    }

    public Optional<Reservation> findReservation(UUID id) {
        try (var connection = dataSource.getConnection();
             var query = connection.prepareStatement("SELECT * FROM reservation WHERE id = ?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                return rows.next() ? Optional.of(reservation(rows)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not read reservation", e);
        }
    }

    @Transactional
    public Outcome reserve(UUID saleId, UUID requestKey) {
        UUID reservationId = UUID.randomUUID();
        try (var connection = dataSource.getConnection()) {
            if (!claimRequest(connection, saleId, requestKey, reservationId)) {
                // At PostgreSQL READ COMMITTED, this new statement sees a committed
                // winner even if the INSERT had to wait for a concurrent request.
                return previousRequest(connection, saleId, requestKey);
            }
            try (var update = connection.prepareStatement("""
                    UPDATE sale SET remaining = remaining - 1
                    WHERE id = ? AND remaining > 0
                    """)) {
                update.setObject(1, saleId);
                if (update.executeUpdate() != 1) {
                    throw new Rejected(Rejection.SOLD_OUT);
                }
            }
            writeAcceptance(connection, saleId, reservationId);
            return new Outcome(new Reservation(reservationId, saleId, "PENDING"), true);
        } catch (SQLException e) {
            throw new IllegalStateException("Could not persist reservation and acceptance event", e);
        }
    }

    private static boolean claimRequest(Connection connection, UUID saleId,
                                        UUID requestKey, UUID reservationId) throws SQLException {
        try (var insert = connection.prepareStatement("""
                INSERT INTO reservation (id, sale_id, request_key)
                SELECT ?, id, ? FROM sale WHERE id = ?
                ON CONFLICT (request_key) DO NOTHING
                """)) {
            insert.setObject(1, reservationId);
            insert.setObject(2, requestKey);
            insert.setObject(3, saleId);
            return insert.executeUpdate() == 1;
        }
    }

    private static Outcome previousRequest(Connection connection, UUID saleId,
                                           UUID requestKey) throws SQLException {
        try (var query = connection.prepareStatement("SELECT * FROM reservation WHERE request_key = ?")) {
            query.setObject(1, requestKey);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new Rejected(Rejection.SALE_NOT_FOUND);
                }
                Reservation previous = reservation(rows);
                if (!saleId.equals(previous.saleId())) {
                    throw new Rejected(Rejection.IDEMPOTENCY_CONFLICT);
                }
                return new Outcome(previous, false);
            }
        }
    }

    private static Reservation reservation(ResultSet rows) throws SQLException {
        return new Reservation(rows.getObject("id", UUID.class), rows.getObject("sale_id", UUID.class),
                rows.getString("status"));
    }

    private static void writeAcceptance(Connection connection, UUID saleId,
                                        UUID reservationId) throws SQLException {
        UUID eventId = UUID.randomUUID();
        try (var insert = connection.prepareStatement("""
                INSERT INTO outbox (event_id, reservation_id, event_type, payload)
                VALUES (?, ?, 'reservation.accepted.v1', jsonb_build_object(
                    'eventId', CAST(? AS uuid), 'reservationId', CAST(? AS uuid),
                    'saleId', CAST(? AS uuid), 'type', 'reservation.accepted.v1',
                    'occurredAt', NOW()
                ))
                """)) {
            insert.setObject(1, eventId);
            insert.setObject(2, reservationId);
            insert.setObject(3, eventId);
            insert.setObject(4, reservationId);
            insert.setObject(5, saleId);
            insert.executeUpdate();
        }
    }
}
