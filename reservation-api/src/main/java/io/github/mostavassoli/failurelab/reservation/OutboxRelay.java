package io.github.mostavassoli.failurelab.reservation;

import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
@UnlessBuildProfile("test")
class OutboxRelay {
    private final OutboxPublisher publisher;

    OutboxRelay(OutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(every = "1s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void publishPending() {
        // Each call commits independently; cap work per tick in the local lab.
        for (int i = 0; i < 16 && publisher.publishOne(); i++) {
            // Stop as soon as the outbox is empty.
        }
    }
}
