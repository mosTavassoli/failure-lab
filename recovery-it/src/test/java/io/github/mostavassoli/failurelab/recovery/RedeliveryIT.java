package io.github.mostavassoli.failurelab.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

class RedeliveryIT {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String TOPIC = "reservation.accepted.v1";
    private static final String GROUP = "failure-lab-fulfillment";
    private static final String KAFKA_GROUP = "Kafka consumer group";
    private static final String WORKER_LOG = "Fulfillment worker log";
    private static final String HTTP_STATUS = "Reservation API status";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Pattern HTTP_PORT = Pattern.compile("Listening on: http://127\\.0\\.0\\.1:(\\d+)");
    private final List<Map<String, Object>> timeline = new ArrayList<>();

    @TempDir
    Path temp;

    @Test
    void forcedProcessExitAfterDatabaseCommitRedeliversWithoutDuplicateOrMissingTickets() throws Exception {
        try (var db = new PostgreSQLContainer("postgres:17-alpine");
             var kafka = new KafkaContainer("apache/kafka:4.1.1")) {
            db.start();
            kafka.start();
            try (var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()));
                 var http = HttpClient.newHttpClient()) {
                admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
                var environment = Map.of(
                        "QUARKUS_DATASOURCE_JDBC_URL", db.getJdbcUrl(),
                        "QUARKUS_DATASOURCE_USERNAME", db.getUsername(),
                        "QUARKUS_DATASOURCE_PASSWORD", db.getPassword(),
                        "KAFKA_BOOTSTRAP_SERVERS", kafka.getBootstrapServers());
                try (var api = start("reservation-api", "api", environment, Map.of(
                        "QUARKUS_HTTP_HOST", "127.0.0.1", "QUARKUS_HTTP_PORT", "0"))) {
                    URI base = api.awaitHttp();
                    UUID sale = seedSale(db);
                    UUID first = reserve(http, base, sale);
                    record("first_accepted", "The API accepted the first reservation.",
                            List.of("Reservation API (HTTP 201)"), Map.of("reservation_id", first));
                    var firstDelivery = readFirstDelivery(kafka.getBootstrapServers());
                    assertEquals(first.toString(), firstDelivery.key());
                    var event = JSON.readTree(firstDelivery.value());
                    UUID eventId = UUID.fromString(event.path("eventId").asText());
                    assertEquals(first.toString(), event.path("reservationId").asText());
                    assertEquals(sale.toString(), event.path("saleId").asText());
                    assertEquals(TOPIC, event.path("type").asText());
                    await("relay marks the broker-acknowledged outbox row", api,
                            () -> published(db, eventId));
                    record("event_published", "The relay published the first acceptance event.",
                            List.of("Kafka record", "PostgreSQL outbox"),
                            Map.of("reservation_id", first, "event_id", eventId,
                                    "partition", firstDelivery.partition(), "event_offset", firstDelivery.offset()));

                    Ticket committedBeforeCrash;
                    try (var crashing = start("fulfillment-worker", "worker-before-crash", environment, Map.of(
                            "FAILURE_LAB_LAB_MODE", "true", "FAILURE_LAB_HOLD_ACK_FOR", first.toString()))) {
                        crashing.awaitLog("Lab: acknowledgement held after commit for reservation " + first);
                        committedBeforeCrash = ticket(db, first);
                        assertNotNull(committedBeforeCrash, "ticket must already be durable");
                        assertEquals(eventId, committedBeforeCrash.eventId());
                        assertEquals("ISSUED", status(http, base, first));
                        var facts = offsetFacts(firstDelivery.offset(), uncommittedOffset(admin, firstDelivery.offset()));
                        facts.put("reservation_id", first);
                        facts.put("ticket_id", committedBeforeCrash.id());
                        facts.put("status", "ISSUED");
                        record("ack_held", "The ticket was durable and visible as ISSUED, but acknowledgement was held.",
                                List.of(WORKER_LOG, "PostgreSQL", HTTP_STATUS, KAFKA_GROUP), facts);
                        crashing.kill();
                        Long afterCrash = uncommittedOffset(admin, firstDelivery.offset());
                        record("worker_crashed", "The drill killed the worker; the offset still had not advanced.",
                                List.of("Lab process harness", KAFKA_GROUP),
                                offsetFacts(firstDelivery.offset(), afterCrash));
                    }

                    UUID second = reserve(http, base, sale);
                    assertEquals("PENDING", status(http, base, second));
                    record("second_pending", "The API accepted a second reservation; it was PENDING without a worker.",
                            List.of("Reservation API (HTTP 201)", HTTP_STATUS),
                            Map.of("reservation_id", second, "status", "PENDING"));
                    try (var restarted = start("fulfillment-worker", "worker-after-crash", environment, Map.of())) {
                        restarted.awaitLog("Ticket already issued for reservation " + first + " (event " + eventId + ")");
                        Ticket afterRedelivery = ticket(db, first);
                        assertEquals(committedBeforeCrash, afterRedelivery);
                        record("redelivered", "The restarted worker saw the same event and kept the original ticket.",
                                List.of(WORKER_LOG, "PostgreSQL"),
                                Map.of("reservation_id", first, "event_id", eventId, "ticket_id", afterRedelivery.id()));
                        restarted.awaitLog("Ticket issued for reservation " + second);
                        long nextOffset = firstDelivery.offset() + 2;
                        await("worker commits offsets for both reservations", restarted, () -> {
                            var offset = committedOffset(admin);
                            return offset != null && offset.offset() == nextOffset;
                        });
                        record("offsets_committed", "Kafka recorded consumer progress past both events.",
                                List.of(KAFKA_GROUP), Map.of("next_offset", nextOffset));
                        assertEquals("ISSUED", status(http, base, second));
                        record("second_issued", "The API confirmed that the restarted worker issued the second reservation.",
                                List.of(WORKER_LOG, HTTP_STATUS), Map.of("reservation_id", second, "status", "ISSUED"));
                        var accepted = List.of(first.toString(), second.toString());
                        var ledger = ledger(db);
                        var result = TicketVerification.verify(accepted, ledger);
                        boolean passed = Boolean.TRUE.equals(result.get("passed"));
                        if (passed) {
                            for (var row : ledger.get("tickets")) {
                                if (first.toString().equals(row.get("reservation_id").asText())) {
                                    assertEquals(committedBeforeCrash.id().toString(), row.get("id").asText(),
                                            "final ledger must retain the original ticket");
                                }
                            }
                        }
                        record("tickets_reconciled", "Checked accepted reservation IDs against the PostgreSQL ledger.",
                                List.of("Reservation API", "PostgreSQL"),
                                Map.of("accepted", result.get("accepted"), "tickets", result.get("tickets"), "passed", passed));
                        writeReport(accepted, ledger, result);
                        assertTrue(passed, () -> "Ticket reconciliation: " + result);
                    }
                }
            }
        }
    }

    private static Long uncommittedOffset(Admin admin, long eventOffset) throws Exception {
        var committed = committedOffset(admin);
        assertTrue(committed == null || committed.offset() <= eventOffset,
                "consumer offset must not have advanced past the held event");
        return committed == null ? null : committed.offset();
    }

    private static Map<String, Object> offsetFacts(long eventOffset, Long committed) {
        var facts = new LinkedHashMap<String, Object>();
        facts.put("event_offset", eventOffset);
        facts.put("committed_next_offset", committed);
        return facts;
    }

    private void record(String stage, String summary, List<String> sources, Map<String, ?> facts) {
        timeline.add(Map.of("stage", stage, "observed_at", Instant.now().truncatedTo(ChronoUnit.MILLIS).toString(),
                "summary", summary, "sources", sources, "facts", facts));
    }

    private void writeReport(List<String> accepted, JsonNode ledger, Map<String, Object> result) throws Exception {
        var report = Map.of(
                "format", "failure-lab/incident-report-v1",
                "scenario", "worker-redelivery",
                "title", "Worker crash after commit, before acknowledgement",
                "target", "fulfillment-worker",
                "runbook", "knowledge/worker-redelivery.md",
                "timeline", timeline,
                "diagnosis", Map.of("summary", "The worker committed the ticket before it could acknowledge the event.",
                        "evidence_stages", List.of("ack_held", "worker_crashed", "redelivered")),
                "verification", Map.of("accepted_ids", accepted, "ledger", ledger, "result", result));
        Path output = Path.of(System.getProperty("failurelab.reportDir"), "incident-report.json");
        String json = JSON.writerWithDefaultPrettyPrinter().with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
                .writeValueAsString(report);
        Files.writeString(output, json + "\n");
    }

    private static OffsetAndMetadata committedOffset(Admin admin) throws Exception {
        return admin.listConsumerGroupOffsets(GROUP).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).get(PARTITION);
    }

    private static org.apache.kafka.clients.consumer.ConsumerRecord<String, String> readFirstDelivery(
            String bootstrap) {
        try (var probe = new KafkaConsumer<>(Map.of(
                "bootstrap.servers", bootstrap, "enable.auto.commit", false),
                new StringDeserializer(), new StringDeserializer())) {
            probe.assign(List.of(PARTITION));
            probe.seekToBeginning(List.of(PARTITION));
            long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
            while (System.nanoTime() < deadline) {
                var records = probe.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("No event was published to " + TOPIC);
    }

    private static UUID seedSale(PostgreSQLContainer db) throws Exception {
        UUID sale = UUID.randomUUID();
        try (var connection = connect(db);
             var insert = connection.prepareStatement(
                     "INSERT INTO sale (id, name, capacity, remaining) VALUES (?, 'Recovery test', 2, 2)")) {
            insert.setObject(1, sale);
            insert.executeUpdate();
        }
        return sale;
    }

    private static UUID reserve(HttpClient client, URI base, UUID sale) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve("/api/reservations")).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString("{\"saleId\":\"" + sale + "\"}")).build();
        var result = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, result.statusCode(), result.body());
        return UUID.fromString(JSON.readTree(result.body()).path("id").asText());
    }

    private static String status(HttpClient client, URI base, UUID reservation) throws Exception {
        var response = client.send(HttpRequest.newBuilder(base.resolve("/api/reservations/" + reservation))
                .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), response.body());
        return JSON.readTree(response.body()).path("status").asText();
    }

    private static boolean published(PostgreSQLContainer db, UUID eventId) throws Exception {
        try (var connection = connect(db);
             var query = connection.prepareStatement("SELECT published_at FROM outbox WHERE event_id = ?")) {
            query.setObject(1, eventId);
            try (var rows = query.executeQuery()) {
                return rows.next() && rows.getTimestamp(1) != null;
            }
        }
    }

    private static Ticket ticket(PostgreSQLContainer db, UUID reservation) throws Exception {
        try (var connection = connect(db);
             var query = connection.prepareStatement("SELECT id, source_event_id FROM ticket WHERE reservation_id = ?")) {
            query.setObject(1, reservation);
            try (var rows = query.executeQuery()) {
                return rows.next() ? new Ticket(rows.getObject(1, UUID.class), rows.getObject(2, UUID.class)) : null;
            }
        }
    }

    private static JsonNode ledger(PostgreSQLContainer db) throws Exception {
        var sql = Files.readString(Path.of(System.getProperty("failurelab.root"), "lab", "ticket-ledger.sql"));
        try (var connection = connect(db); var query = connection.prepareStatement(sql)) {
            try (var rows = query.executeQuery()) {
                assertTrue(rows.next(), "the ledger query must return one snapshot");
                return JSON.readTree(rows.getString(1));
            }
        }
    }

    private static java.sql.Connection connect(PostgreSQLContainer db) throws Exception {
        return DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
    }

    private App start(String module, String name, Map<String, String> environment,
                      Map<String, String> overrides) throws Exception {
        Path jar = Path.of(System.getProperty("failurelab.root"), module,
                "target", "quarkus-app", "quarkus-run.jar").toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(jar), "Missing packaged app: " + jar);
        boolean windows = System.getProperty("os.name").startsWith("Windows");
        var java = Path.of(System.getProperty("java.home"), "bin", windows ? "java.exe" : "java");
        Path log = temp.resolve(name + ".log");
        var builder = new ProcessBuilder(java.toString(), "-Dquarkus.log.console.color=false", "-jar", jar.toString());
        builder.environment().putAll(environment);
        builder.environment().putAll(overrides);
        builder.redirectErrorStream(true).redirectOutput(log.toFile());
        return new App(builder.start(), log);
    }

    private static void await(String description, App app, CheckedCheck check) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(35).toNanos();
        do {
            assertTrue(app.process().isAlive(), () -> app.log() + " exited:\n" + app.output());
            if (check.ready()) {
                return;
            }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Timed out waiting for " + description + ":\n" + app.output());
    }

    @FunctionalInterface
    private interface CheckedCheck {
        boolean ready() throws Exception;
    }

    private record Ticket(UUID id, UUID eventId) {}

    private record App(Process process, Path log) implements AutoCloseable {
        String output() {
            try {
                return Files.readString(log);
            } catch (java.io.IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        URI awaitHttp() throws Exception {
            await("the API to listen", this, () -> HTTP_PORT.matcher(output()).find());
            var match = HTTP_PORT.matcher(output());
            assertTrue(match.find());
            return URI.create("http://127.0.0.1:" + match.group(1));
        }

        void awaitLog(String line) throws Exception {
            await(line, this, () -> output().contains(line));
        }

        void kill() throws InterruptedException {
            if (process.isAlive()) {
                process.destroyForcibly();
                assertTrue(process.waitFor(10, TimeUnit.SECONDS));
            }
            assertFalse(process.isAlive());
        }

        @Override
        public void close() throws InterruptedException {
            kill();
        }
    }
}
