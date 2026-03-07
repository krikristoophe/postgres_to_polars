CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    tags TEXT[] DEFAULT '{}',
    birth_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    login_time TIME DEFAULT '08:00:00'
);

INSERT INTO users (first_name, last_name, email, tags, birth_date, created_at, login_time)
SELECT
    'first_' || i,
    'last_' || i,
    'user_' || i || '@example.com',
    ARRAY['tag_' || (i % 10), 'group_' || (i % 5)],
    '1990-01-01'::date + (i % 10000),
    '2020-01-01 00:00:00'::timestamp + (i || ' seconds')::interval,
    '08:00:00'::time + ((i % 3600) || ' seconds')::interval
FROM generate_series(1, 500000) i;
