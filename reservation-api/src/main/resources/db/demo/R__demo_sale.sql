INSERT INTO sale (id, name, capacity, remaining)
VALUES ('f0000000-0000-4000-8000-000000000001', 'Failure Lab demo', 12, 12)
ON CONFLICT (id) DO NOTHING;
