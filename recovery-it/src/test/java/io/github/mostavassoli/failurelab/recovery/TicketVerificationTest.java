package io.github.mostavassoli.failurelab.recovery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class TicketVerificationTest {
    @Test
    void duplicateAndMissingTicketsFailEvenWhenCountsMatch() throws Exception {
        var ledger = new ObjectMapper().readTree("""
                {
                  "reservations": [
                    {"id": "a", "status": "ISSUED", "event_id": "event-a"},
                    {"id": "b", "status": "ISSUED", "event_id": "event-b"}
                  ],
                  "tickets": [
                    {"id": "t1", "reservation_id": "a", "source_event_id": "event-a"},
                    {"id": "t2", "reservation_id": "a", "source_event_id": "event-a"}
                  ]
                }
                """);
        var result = TicketVerification.verify(List.of("a", "b"), ledger);
        assertEquals(result.get("accepted"), result.get("tickets"));
        assertEquals(false, result.get("passed"));
        assertEquals(List.of("b"), result.get("missing_tickets"));
        assertEquals(List.of("a"), result.get("duplicate_tickets"));
    }
}
