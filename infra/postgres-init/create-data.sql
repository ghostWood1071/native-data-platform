
CREATE DATABASE retail;
\c retail;

CREATE TABLE customer (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    address VARCHAR(200),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP - INTERVAL '1 days',
    update_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP - INTERVAL '1 days',
    action CHAR(1) CHECK (action IN ('i', 'd', 'u'))
);


CREATE TABLE "order" (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customer(id) ON DELETE CASCADE,
    address VARCHAR(200),
    amount DECIMAL(10, 2),
    tax DECIMAL(10, 2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP - INTERVAL '1 days',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP - INTERVAL '1 days',
    action CHAR(1) CHECK (action IN ('i', 'd', 'u'))
);


DO $$
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO customer (name, address, age, action)
        VALUES
        (
            'Customer ' || i,
            'Address ' || i,
            (FLOOR(RANDOM() * (60) + 18))::INT,
            CASE WHEN i % 2 = 0 THEN 'i' ELSE 'u' END
        );
    END LOOP;
END $$;


DO $$
BEGIN
    FOR i IN 1..200 LOOP
        INSERT INTO "order" (customer_id, address, amount, tax, status, action)
        VALUES
        (
            (FLOOR(RANDOM() * 200) + 1)::INT,
            'Order Address ' || i,
            (FLOOR(RANDOM() * (1000) + 100))::DECIMAL,
            (FLOOR(RANDOM() * (200) + 10))::DECIMAL,
            CASE WHEN i % 2 = 0 THEN 'Completed' ELSE 'Pending' END,
            CASE WHEN i % 2 = 0 THEN 'i' ELSE 'u' END
        );
    END LOOP;
END $$;

CREATE USER etl WITH PASSWORD 'etl';
GRANT CONNECT ON DATABASE retail TO etl;
GRANT USAGE ON SCHEMA public TO etl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO etl;


