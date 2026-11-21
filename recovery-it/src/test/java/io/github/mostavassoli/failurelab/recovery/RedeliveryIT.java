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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    private static final Pattern HTTP_PORT = Pattern.compile("Listening on: http://127\\.0\\.0\\.1:(\\d+)");

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
                    var firstDelivery = readFirstDelivery(kafka.getBootstrapServers());
                    assertEquals(first.toString(), firstDelivery.key());
                    var event = JSON.readTree(firstDelivery.value());
                    UUID eventId = UUID.fromString(event.path("eventId").asText());
                    assertEquals(first.toString(), event.path("reservationId").asText());
                    assertEquals(sale.toString(), event.path("saleId").asText());
                    assertEquals(TOPIC, event.path("type").asText());
                    await("relay marks the broker-acknowledged outbox row", api,
                            () -> published(db, eventId));

                    Ticket committedBeforeCrash;
                    try (var crashing = start("fulfillment-worker", "worker-before-crash", environment, Map.of(
                            "FAILURE_LAB_LAB_MODE", "true", "FAILURE_LAB_HOLD_ACK_FOR", first.toString()))) {
                        crashing.awaitLog("Lab: acknowledgement held after commit for reservation " + first);
                        committedBeforeCrash = ticket(db, first);
                        assertNotNull(committedBeforeCrash, "ticket must already be durable");
                        assertEquals(eventId, committedBeforeCrash.eventId());
                        assertEquals("ISSUED", status(http, base, first));
                        assertUncommitted(admin, firstDelivery.offset());
                        crashing.kill();
                        assertUncommitted(admin, firstDelivery.offset());
                    }

                    UUID second = reserve(http, base, sale);
                    assertEquals("PENDING", status(http, base, second));
                    try (var restarted = start("fulfillment-worker", "worker-after-crash", environment, Map.of())) {
                        restarted.awaitLog("Ticket already issued for reservation " + first + " (event " + eventId + ")");
                        restarted.awaitLog("Ticket issued for reservation " + second);
                        await("worker commits offsets for both reservations", restarted, () -> {
                            var offset = committedOffset(admin);
                            return offset != null && offset.offset() == firstDelivery.offset() + 2;
                        });
                        assertEquals(committedBeforeCrash, ticket(db, first));
                        assertEquals("ISSUED", status(http, base, second));
                        assertReconciled(db, sale, Set.of(first, second));
                    }
                }
            }
        }
    }

    private static void assertUncommitted(Admin admin, long eventOffset) throws Exception {
        var committed = committedOffset(admin);
        assertTrue(committed == null || committed.offset() <= eventOffset,
                "consumer offset must not have advanced past the held event");
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

    private static void assertReconciled(PostgreSQLContainer db, UUID sale, Set<UUID> accepted) throws Exception {
        var actual = new java.util.HashSet<UUID>();
        try (var connection = connect(db);
             var query = connection.prepareStatement("""
                     SELECT r.id, r.status, t.id AS ticket_id FROM reservation r
                     LEFT JOIN ticket t ON t.reservation_id = r.id WHERE r.sale_id = ?
                     """)) {
            query.setObject(1, sale);
            try (var rows = query.executeQuery()) {
                while (rows.next()) {
                    UUID id = rows.getObject("id", UUID.class);
                    assertTrue(actual.add(id), "duplicate ticket for " + id);
                    assertEquals("ISSUED", rows.getString("status"));
                    assertNotNull(rows.getObject("ticket_id"), "missing ticket for " + id);
                }
            }
        }
        assertEquals(accepted, actual);
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
