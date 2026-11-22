package io.github.mostavassoli.failurelab.recovery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;

final class TicketVerification {
    private TicketVerification() {}

    // Same checks as the offline Python verifier; mvn verify doesn't need Python.
    static Map<String, Object> verify(List<String> accepted, JsonNode ledger) {
        var expected = new HashSet<>(accepted);
        var reservations = new HashMap<String, JsonNode>();
        var reservationIds = new ArrayList<String>();
        for (var reservation : ledger.get("reservations")) {
            String id = reservation.get("id").asText();
            reservationIds.add(id);
            reservations.put(id, reservation);
        }
        var ticketIds = new ArrayList<String>();
        var ticketReservations = new ArrayList<String>();
        var unexpectedTickets = new ArrayList<String>();
        var unmatchedEvents = new ArrayList<String>();
        for (var ticket : ledger.get("tickets")) {
            String id = ticket.get("id").asText();
            String reservationId = ticket.get("reservation_id").asText();
            String eventId = ticket.get("source_event_id").asText("");
            ticketIds.add(id);
            ticketReservations.add(reservationId);
            if (!expected.contains(reservationId)) {
                unexpectedTickets.add(id);
            }
            var reservation = reservations.get(reservationId);
            if (eventId.isEmpty() || reservation == null || !eventId.equals(reservation.get("event_id").asText(""))) {
                unmatchedEvents.add(id);
            }
        }

        var problems = new LinkedHashMap<String, List<String>>();
        problems.put("duplicate_accepted_ids", duplicates(accepted));
        problems.put("missing_reservations", difference(expected, reservations.keySet()));
        problems.put("unexpected_reservations", difference(reservations.keySet(), expected));
        problems.put("duplicate_reservations", duplicates(reservationIds));
        problems.put("not_issued", expected.stream().filter(id -> reservations.containsKey(id)
                && !"ISSUED".equals(reservations.get(id).get("status").asText())).sorted().toList());
        problems.put("missing_tickets", difference(expected, new HashSet<>(ticketReservations)));
        problems.put("duplicate_tickets", duplicates(ticketReservations));
        problems.put("duplicate_ticket_ids", duplicates(ticketIds));
        problems.put("unexpected_tickets", unexpectedTickets.stream().sorted().toList());
        problems.put("unmatched_source_events", unmatchedEvents.stream().sorted().toList());

        var result = new LinkedHashMap<String, Object>();
        result.put("passed", !expected.isEmpty() && problems.values().stream().allMatch(List::isEmpty));
        result.put("accepted", accepted.size());
        result.put("reservations", reservations.size());
        result.put("tickets", ticketIds.size());
        result.putAll(problems);
        return result;
    }

    private static List<String> duplicates(List<String> ids) {
        var seen = new HashSet<String>();
        var repeated = new TreeSet<String>();
        for (String id : ids) {
            if (!seen.add(id)) {
                repeated.add(id);
            }
        }
        return List.copyOf(repeated);
    }

    private static List<String> difference(Set<String> left, Set<String> right) {
        return left.stream().filter(id -> !right.contains(id)).sorted().toList();
    }
}
