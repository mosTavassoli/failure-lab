CREATE TABLE ticket (
    id UUID PRIMARY KEY,
    reservation_id UUID NOT NULL UNIQUE REFERENCES reservation (id),
    source_event_id UUID NOT NULL UNIQUE,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX outbox_pending_order ON outbox (created_at, event_id)
WHERE published_at IS NULL;
