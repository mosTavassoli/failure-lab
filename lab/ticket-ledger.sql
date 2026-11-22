-- One statement gives both drills a consistent snapshot of the disposable database.
SELECT jsonb_build_object(
  'reservations', COALESCE((SELECT jsonb_agg(to_jsonb(acceptance) ORDER BY id) FROM (
      SELECT r.id, r.status, o.event_id
      FROM reservation r LEFT JOIN outbox o
        ON o.reservation_id = r.id AND o.event_type = 'reservation.accepted.v1'
  ) acceptance), '[]'),
  'tickets', COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY reservation_id, id)
                        FROM (SELECT id, reservation_id, source_event_id FROM ticket) t), '[]')
);
