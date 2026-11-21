package io.github.mostavassoli.failurelab.fulfillment;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.Startup;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.jboss.logging.Logger;

@Startup
@ApplicationScoped
class TicketConsumer {
    private static final Logger LOG = Logger.getLogger(TicketConsumer.class);
    private final ObjectMapper json;
    private final TicketStore store;
    private final Optional<UUID> holdAckFor;
    private final int processingDelayMs;

    TicketConsumer(ObjectMapper json, TicketStore store,
                   @ConfigProperty(name = "failure-lab.lab-mode", defaultValue = "false") boolean labMode,
                   @ConfigProperty(name = "failure-lab.hold-ack-for") Optional<UUID> holdAckFor,
                   @ConfigProperty(name = "failure-lab.processing-delay-ms", defaultValue = "0") int processingDelayMs) {
        if (!labMode && (holdAckFor.isPresent() || processingDelayMs != 0)) {
            throw new IllegalStateException("Worker fault controls require explicit lab mode");
        }
        if (processingDelayMs < 0 || processingDelayMs > 5000) {
            throw new IllegalArgumentException("Lab processing delay must be between 0 and 5000 ms");
        }
        this.json = json;
        this.store = store;
        this.holdAckFor = holdAckFor;
        this.processingDelayMs = processingDelayMs;
    }

    @Incoming("reservations")
    @Acknowledgment(Acknowledgment.Strategy.MANUAL)
    @Blocking
    CompletionStage<Void> consume(KafkaRecord<String, String> message) {
        try {
            var event = parse(message);
            // Model bounded downstream latency so a small, synthetic burst can build real lag.
            // Do not hold a database transaction or burn CPU during the delay.
            TimeUnit.MILLISECONDS.sleep(processingDelayMs);
            // The database transaction is committed when this call returns.
            boolean created = store.issue(event);
            LOG.infof("Ticket %s for reservation %s (event %s)",
                    created ? "issued" : "already issued", event.reservationId(), event.eventId());
            if (holdAckFor.filter(event.reservationId()::equals).isPresent()) {
                LOG.infof("Lab: acknowledgement held after commit for reservation %s", event.reservationId());
                // Lab harness kills only this disposable process, without a graceful shutdown.
                return new CompletableFuture<>();
            }
            return message.ack();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return message.nack(e);
        } catch (RuntimeException e) {
            return message.nack(e);
        }
    }

    private ReservationAccepted parse(KafkaRecord<String, String> message) {
        String payload = message.getPayload();
        if (payload == null || payload.length() > 4096) {
            throw new IllegalArgumentException("Missing or oversized acceptance event");
        }
        try {
            var event = json.readValue(payload, ReservationAccepted.class);
            if (event == null || !event.reservationId().toString().equals(message.getKey())) {
                throw new IllegalArgumentException("Kafka key does not match the reservation");
            }
            return event;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Malformed acceptance event");
        }
    }
}
