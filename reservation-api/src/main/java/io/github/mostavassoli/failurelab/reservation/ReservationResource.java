package io.github.mostavassoli.failurelab.reservation;

import java.net.URI;
import java.util.UUID;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
public class ReservationResource {
    private final ReservationStore store;

    public ReservationResource(ReservationStore store) {
        this.store = store;
    }

    public record Request(String saleId) {}

    public record ErrorBody(String code) {}

    @GET
    @Path("/sales/{id}")
    public Response sale(@PathParam("id") String id) {
        UUID saleId = parseId(id);
        return saleId == null ? error(400, "INVALID_SALE_ID") :
                store.findSale(saleId).map(value -> Response.ok(value).build())
                        .orElseGet(() -> error(404, "SALE_NOT_FOUND"));
    }

    @GET
    @Path("/reservations/{id}")
    public Response reservation(@PathParam("id") String id) {
        UUID reservationId = parseId(id);
        return reservationId == null ? error(400, "INVALID_RESERVATION_ID") :
                store.findReservation(reservationId).map(value -> Response.ok(value).build())
                        .orElseGet(() -> error(404, "RESERVATION_NOT_FOUND"));
    }

    @POST
    @Path("/reservations")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response reserve(@HeaderParam("Idempotency-Key") String key, Request request) {
        UUID requestKey = parseId(key);
        UUID saleId = request == null ? null : parseId(request.saleId());
        if (requestKey == null) {
            return error(400, "INVALID_IDEMPOTENCY_KEY");
        }
        if (saleId == null) {
            return error(400, "INVALID_SALE_ID");
        }
        try {
            var outcome = store.reserve(saleId, requestKey);
            if (outcome.created()) {
                return Response.created(URI.create("/api/reservations/" + outcome.reservation().id()))
                        .entity(outcome.reservation()).build();
            }
            return Response.ok(outcome.reservation()).build();
        } catch (ReservationStore.Rejected rejected) {
            return error(rejected.reason == ReservationStore.Rejection.SALE_NOT_FOUND ? 404 : 409,
                    rejected.reason.name());
        }
    }

    private static UUID parseId(String text) {
        if (text == null || text.length() != 36) {
            return null;
        }
        try {
            UUID id = UUID.fromString(text);
            return id.toString().equalsIgnoreCase(text) ? id : null;
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private static Response error(int status, String code) {
        return Response.status(status).entity(new ErrorBody(code)).build();
    }
}
