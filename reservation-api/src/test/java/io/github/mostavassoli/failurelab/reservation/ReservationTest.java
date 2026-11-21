package io.github.mostavassoli.failurelab.reservation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
class ReservationTest {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(25);

    @Inject
    DataSource dataSource;
    @Inject
    ObjectMapper json;
    @TestHTTPResource
    URI base;

    @Test
    void onlyOneRequestReservesTheLastTicketUnderContention() throws Exception {
        UUID sale = createSale(1);
        UUID firstKey = UUID.randomUUID();
        UUID secondKey = UUID.randomUUID();
        try (var client = HttpClient.newHttpClient();
             var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try {
                // Do not release the last ticket until both independent HTTP requests
                // are inside PostgreSQL, waiting to execute the competing updates.
                try (var lock = blocker.prepareStatement("SELECT id FROM sale WHERE id = ? FOR NO KEY UPDATE")) {
                    lock.setObject(1, sale);
                    lock.executeQuery().close();
                }
                var first = client.sendAsync(request(sale, firstKey), HttpResponse.BodyHandlers.ofString());
                var second = client.sendAsync(request(sale, secondKey), HttpResponse.BodyHandlers.ofString());
                awaitContenders();
                blocker.commit();

                var a = first.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                var b = second.get(REQUEST_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                var winner = a.statusCode() == 201 ? a : b;
                var loser = winner == a ? b : a;
                assertEquals(201, winner.statusCode(), winner.body());
                assertEquals(409, loser.statusCode(), loser.body());
                assertEquals("SOLD_OUT", json.readTree(loser.body()).path("code").asText());
            } finally {
                blocker.rollback();
            }
        }
        assertEquals(0, remaining(sale));
        assertEquals(1, countReservations(sale));
        assertEquals(1, countEvents(sale));
    }

    @Test
    void retryReturnsTheOriginalReservationAndDoesNotWriteASecondEvent() throws Exception {
        UUID sale = createSale(1);
        UUID key = UUID.randomUUID();
        try (var client = HttpClient.newHttpClient()) {
            var accepted = send(client, sale, key);
            var retried = send(client, sale, key);
            assertEquals(201, accepted.statusCode(), accepted.body());
            assertEquals(200, retried.statusCode(), retried.body());
            assertEquals(json.readTree(accepted.body()), json.readTree(retried.body()));
            var reservationId = UUID.fromString(json.readTree(accepted.body()).path("id").asText());
            var location = accepted.headers().firstValue("Location").orElseThrow();
            var lookup = client.send(HttpRequest.newBuilder(base.resolve(location)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, lookup.statusCode());
            assertEquals(json.readTree(accepted.body()), json.readTree(lookup.body()));
            assertEventPayload(reservationId, sale);
        }
        assertEquals(0, remaining(sale));
        assertEquals(1, countReservations(sale));
        assertEquals(1, countEvents(sale));
    }

    @Test
    void keyCannotBeReusedForAnotherSale() throws Exception {
        UUID sale = createSale(1);
        UUID otherSale = createSale(1);
        UUID key = UUID.randomUUID();
        try (var client = HttpClient.newHttpClient()) {
            assertEquals(201, send(client, sale, key).statusCode());
            var conflict = send(client, otherSale, key);
            assertEquals(409, conflict.statusCode());
            assertEquals("IDEMPOTENCY_CONFLICT", json.readTree(conflict.body()).path("code").asText());
        }
        assertEquals(1, remaining(otherSale));
        assertEquals(0, countReservations(otherSale));
    }

    @Test
    void failedOutboxInsertRollsBackStockAndReservation() throws Exception {
        UUID sale = createSale(1);
        UUID key = UUID.randomUUID();
        // Change only this synthetic sale in the disposable test database.
        try (var connection = dataSource.getConnection();
             var statement = connection.createStatement();
             var client = HttpClient.newHttpClient()) {
            statement.execute("""
                    ALTER TABLE outbox ADD CONSTRAINT test_reject_sale
                    CHECK (payload->>'saleId' <> '%s')
                    """.formatted(sale));
            try {
                assertEquals(500, send(client, sale, key).statusCode());
                assertEquals(1, remaining(sale));
                assertEquals(0, countReservations(sale));
                assertEquals(0, countEvents(sale));
            } finally {
                statement.execute("ALTER TABLE outbox DROP CONSTRAINT test_reject_sale");
            }
            assertEquals(201, send(client, sale, key).statusCode());
            assertEquals(1, countEvents(sale));
        }
    }

    @Test
    void rejectsMalformedKeysAndUnknownSales() throws Exception {
        try (var client = HttpClient.newHttpClient()) {
            var invalid = client.send(request(UUID.randomUUID(), "not-a-uuid"),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(400, invalid.statusCode());
            assertEquals("INVALID_IDEMPOTENCY_KEY", json.readTree(invalid.body()).path("code").asText());
            var missing = send(client, UUID.randomUUID(), UUID.randomUUID());
            assertEquals(404, missing.statusCode());
            assertEquals("SALE_NOT_FOUND", json.readTree(missing.body()).path("code").asText());
        }
    }

    private UUID createSale(int stock) throws SQLException {
        UUID sale = UUID.randomUUID();
        try (var connection = dataSource.getConnection();
             var insert = connection.prepareStatement(
                     "INSERT INTO sale (id, name, capacity, remaining) VALUES (?, 'Test sale', ?, ?)")) {
            insert.setObject(1, sale);
            insert.setInt(2, stock);
            insert.setInt(3, stock);
            insert.executeUpdate();
        }
        return sale;
    }

    private int remaining(UUID sale) throws SQLException {
        return value("SELECT remaining FROM sale WHERE id = ?", sale);
    }

    private int countReservations(UUID sale) throws SQLException {
        return value("SELECT count(*) FROM reservation WHERE sale_id = ?", sale);
    }

    private int countEvents(UUID sale) throws SQLException {
        return value("""
                SELECT count(*) FROM outbox o JOIN reservation r ON r.id = o.reservation_id
                WHERE r.sale_id = ?
                """, sale);
    }

    private int value(String sql, UUID sale) throws SQLException {
        try (var connection = dataSource.getConnection();
             var query = connection.prepareStatement(sql)) {
            query.setObject(1, sale);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Expected one row");
                }
                return rows.getInt(1);
            }
        }
    }

    private void assertEventPayload(UUID id, UUID sale) throws Exception {
        try (var connection = dataSource.getConnection();
             var query = connection.prepareStatement("SELECT event_id, payload FROM outbox WHERE reservation_id = ?")) {
            query.setObject(1, id);
            try (var rows = query.executeQuery()) {
                if (!rows.next()) {
                    fail("Reservation has no outbox event");
                }
                var payload = json.readTree(rows.getString("payload"));
                assertEquals(rows.getString("event_id"), payload.path("eventId").asText());
                assertEquals(id.toString(), payload.path("reservationId").asText());
                assertEquals(sale.toString(), payload.path("saleId").asText());
                assertEquals("reservation.accepted.v1", payload.path("type").asText());
                assertNotNull(payload.get("occurredAt"));
                assertNotEquals("", payload.path("occurredAt").asText());
            }
        }
    }

    private void awaitContenders() throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        try (var connection = dataSource.getConnection();
             var probe = connection.prepareStatement("""
                     SELECT count(*) FROM pg_stat_activity
                     WHERE datname = current_database() AND wait_event_type = 'Lock'
                     AND query LIKE 'UPDATE sale SET remaining%'
                     """)) {
            do {
                try (var rows = probe.executeQuery()) {
                    if (rows.next() && rows.getInt(1) == 2) {
                        return;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(25);
            } while (System.nanoTime() < deadline);
        }
        fail("Expected both reservation requests to be waiting on the PostgreSQL sale lock");
    }

    private HttpRequest request(UUID sale, UUID key) {
        return request(sale, key.toString());
    }

    private HttpRequest request(UUID sale, String key) {
        return HttpRequest.newBuilder(base.resolve("/api/reservations")).timeout(REQUEST_TIMEOUT)
                .header("Content-Type", "application/json").header("Idempotency-Key", key)
                .POST(HttpRequest.BodyPublishers.ofString("{\"saleId\":\"" + sale + "\"}")).build();
    }

    private HttpResponse<String> send(HttpClient client, UUID sale, UUID key) throws Exception {
        return client.send(request(sale, key), HttpResponse.BodyHandlers.ofString());
    }
}
