package io.github.mostavassoli.failurelab.reservation;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;

import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.reactive.messaging.kafka.Record;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.transaction.Transactional;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

@ApplicationScoped
@UnlessBuildProfile("test")
class OutboxPublisher {
    private final DataSource dataSource;
    private final Emitter<Record<String, String>> events;

    OutboxPublisher(DataSource dataSource, @Channel("reservation-events") Emitter<Record<String, String>> events) {
        this.dataSource = dataSource;
        this.events = events;
    }

    @Transactional
    boolean publishOne() {
        try (var connection = dataSource.getConnection();
             var select = connection.prepareStatement("""
                     SELECT event_id, reservation_id, payload
                     FROM outbox WHERE published_at IS NULL
                     ORDER BY created_at, event_id LIMIT 1 FOR UPDATE SKIP LOCKED
                     """)) {
            UUID eventId;
            try (var rows = select.executeQuery()) {
                if (!rows.next()) {
                    return false;
                }
                eventId = rows.getObject("event_id", UUID.class);
                // Retain the exact payload and event ID when retrying a publish.
                send(rows.getString("reservation_id"), rows.getString("payload"));
            }
            try (var mark = connection.prepareStatement("UPDATE outbox SET published_at = NOW() WHERE event_id = ?")) {
                mark.setObject(1, eventId);
                mark.executeUpdate();
            }
            return true;
        } catch (SQLException e) {
            throw new IllegalStateException("Could not advance the outbox", e);
        }
    }

    private void send(String key, String payload) {
        try {
            events.send(Record.of(key, payload)).toCompletableFuture().get(8, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Outbox send was interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Outbox send was not acknowledged", e);
        }
    }
}
