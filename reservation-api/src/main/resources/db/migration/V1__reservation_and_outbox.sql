CREATE TABLE sale (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    remaining INTEGER NOT NULL CHECK (remaining BETWEEN 0 AND capacity)
);

CREATE TABLE reservation (
    id UUID PRIMARY KEY,
    sale_id UUID NOT NULL REFERENCES sale (id),
    request_key UUID NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'ISSUED'))
);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    reservation_id UUID NOT NULL REFERENCES reservation (id),
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    UNIQUE (reservation_id, event_type)
);
