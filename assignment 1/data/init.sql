-- ============================================================
-- NYC Taxi Trips Database Schema + Seed Data
-- Big Data 07 — Assignment 1
-- ============================================================

-- Drop table if exists for clean restarts
DROP TABLE IF EXISTS taxi_trips;

-- Create the main taxi trips table
CREATE TABLE taxi_trips (
    trip_id             SERIAL PRIMARY KEY,
    pickup_datetime     TIMESTAMP NOT NULL,
    dropoff_datetime    TIMESTAMP NOT NULL,
    passenger_count     INTEGER,
    trip_distance       NUMERIC(8,2),
    pickup_longitude    NUMERIC(10,6),
    pickup_latitude     NUMERIC(10,6),
    dropoff_longitude   NUMERIC(10,6),
    dropoff_latitude    NUMERIC(10,6),
    payment_type        VARCHAR(20),
    fare_amount         NUMERIC(8,2),
    tip_amount          NUMERIC(8,2),
    tolls_amount        NUMERIC(8,2),
    total_amount        NUMERIC(8,2),
    vendor_id           VARCHAR(10)
);

-- ============================================================
-- Seed 25,000 realistic NYC taxi trip rows
-- Using PostgreSQL generate_series for reproducible data
-- ============================================================
INSERT INTO taxi_trips (
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    vendor_id
)
SELECT
    -- pickup: spread across 2024 with rush hour bias
    timestamp '2024-01-01 00:00:00' +
        (random() * interval '365 days') +
        (floor(random() * 24) * interval '1 hour'),

    -- dropoff: 5-45 minutes after pickup
    timestamp '2024-01-01 00:00:00' +
        (random() * interval '365 days') +
        (floor(random() * 24) * interval '1 hour') +
        ((5 + random() * 40) * interval '1 minute'),

    -- passengers: 1-6 weighted toward 1-2
    CASE
        WHEN random() < 0.55 THEN 1
        WHEN random() < 0.80 THEN 2
        WHEN random() < 0.92 THEN 3
        WHEN random() < 0.97 THEN 4
        ELSE (5 + floor(random() * 2))::int
    END,

    -- distance: 0.5 to 25 miles, weighted shorter
    round((0.5 + (random()^1.5) * 24.5)::numeric, 2),

    -- pickup longitude: Manhattan bounding box
    round((-74.0200 + random() * 0.0900)::numeric, 6),

    -- pickup latitude: Manhattan bounding box
    round((40.7000 + random() * 0.1200)::numeric, 6),

    -- dropoff longitude
    round((-74.0200 + random() * 0.0900)::numeric, 6),

    -- dropoff latitude
    round((40.7000 + random() * 0.1200)::numeric, 6),

    -- payment type: realistic distribution
    CASE
        WHEN random() < 0.67 THEN 'Credit Card'
        WHEN random() < 0.92 THEN 'Cash'
        WHEN random() < 0.97 THEN 'No Charge'
        ELSE 'Dispute'
    END,

    -- fare: $3 base + ~$2.50 per mile
    round((3.0 + (0.5 + (random()^1.5) * 24.5) * 2.50)::numeric, 2),

    -- tip: 0-25% of fare, only on credit card
    round(
        CASE WHEN random() < 0.67
        THEN (3.0 + random() * 62.5) * (random() * 0.25)
        ELSE 0.0 END
    ::numeric, 2),

    -- tolls: occasional
    round(
        CASE WHEN random() < 0.15
        THEN random() * 5.54
        ELSE 0.0 END
    ::numeric, 2),

    -- total: fare + tip + tolls + $0.50 surcharge
    round((3.0 + (0.5 + (random()^1.5) * 24.5) * 2.50 +
           random() * 8.0 + 0.50)::numeric, 2),

    -- vendor: NYC TLC registered vendors
    (ARRAY['CMT','VTS','DDS'])[1 + floor(random() * 3)::int]

FROM generate_series(1, 25000);

-- ============================================================
-- Indexes for fast producer reads
-- ============================================================
CREATE INDEX idx_pickup_datetime
    ON taxi_trips(pickup_datetime);

CREATE INDEX idx_payment_type
    ON taxi_trips(payment_type);

CREATE INDEX idx_vendor_id
    ON taxi_trips(vendor_id);

-- ============================================================
-- Verify seed data
-- ============================================================
DO $$
DECLARE
    row_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO row_count FROM taxi_trips;
    RAISE NOTICE 'taxi_trips table seeded with % rows', row_count;
END $$;
