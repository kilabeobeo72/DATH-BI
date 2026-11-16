-- ==========================================================
-- Schema: stage (Tầng Staging - Truncate & Reload theo batch)
-- ==========================================================
CREATE TABLE IF NOT EXISTS stage.stg_yellow_trips (
    batch_id              VARCHAR(64) NOT NULL,
    source_file           VARCHAR(255) NOT NULL,
    ingested_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    vendor_id             INTEGER,
    tpep_pickup_datetime  TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count       INTEGER,
    trip_distance         DOUBLE PRECISION,
    ratecode_id           INTEGER,
    store_and_fwd_flag    VARCHAR(5),
    pu_location_id        INTEGER,
    do_location_id        INTEGER,
    payment_type          INTEGER,
    fare_amount           DOUBLE PRECISION,
    extra                 DOUBLE PRECISION,
    mta_tax               DOUBLE PRECISION,
    tip_amount            DOUBLE PRECISION,
    tolls_amount          DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount          DOUBLE PRECISION,
    congestion_surcharge  DOUBLE PRECISION,
    airport_fee           DOUBLE PRECISION
);

-- ==========================================================
-- Schema: dds (8 Bảng Dimension + 1 Bảng Fact_TaxiTrip)
-- ==========================================================

-- 1. Dim_Date (732 ngày: 2023-01-01 đến 2025-01-01 cho chuyến xuyên đêm)
CREATE TABLE IF NOT EXISTS dds.dim_date (
    date_key            INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date           DATE NOT NULL UNIQUE,
    day_number_of_week  SMALLINT NOT NULL,        -- 1 (Thứ Hai) .. 7 (Chủ Nhật)
    day_name_of_week    VARCHAR(15) NOT NULL,
    day_number_of_month SMALLINT NOT NULL,
    day_number_of_year  SMALLINT NOT NULL,
    week_number_of_year SMALLINT NOT NULL,
    month_number        SMALLINT NOT NULL,
    month_name          VARCHAR(15) NOT NULL,
    quarter_number      SMALLINT NOT NULL,
    quarter_name        CHAR(2) NOT NULL,         -- Q1..Q4
    year_number         SMALLINT NOT NULL,
    year_month          CHAR(7) NOT NULL,         -- YYYY-MM
    is_weekend          SMALLINT NOT NULL,        -- 0 | 1
    is_holiday          SMALLINT NOT NULL         -- 0 | 1
);

-- 2. Dim_Time (1440 phút trong ngày)
CREATE TABLE IF NOT EXISTS dds.dim_time (
    time_key            INTEGER PRIMARY KEY,      -- HHMM (0 .. 2359)
    time_value          TIME NOT NULL UNIQUE,
    hour_24             SMALLINT NOT NULL,
    minute_of_hour      SMALLINT NOT NULL,
    hour_12             SMALLINT NOT NULL,
    am_pm               CHAR(2) NOT NULL,
    time_of_day         VARCHAR(15) NOT NULL,     -- Đêm, Sáng, Trưa, Chiều, Tối
    peak_hour_category  VARCHAR(30) NOT NULL      -- Cao điểm sáng, Giờ hành chính, ...
);

-- 3. Dim_Location (265 khu vực từ taxi_zone_lookup.csv)
CREATE TABLE IF NOT EXISTS dds.dim_location (
    location_key        INTEGER PRIMARY KEY,      -- 1..265
    location_id         INTEGER NOT NULL UNIQUE,
    zone_name           VARCHAR(80) NOT NULL,
    borough             VARCHAR(30) NOT NULL,
    service_zone        VARCHAR(30) NOT NULL
);

-- 4. Dim_Vendor
CREATE TABLE IF NOT EXISTS dds.dim_vendor (
    vendor_key          SMALLINT PRIMARY KEY,     -- 1, 2, 3
    vendor_id           INTEGER NOT NULL UNIQUE,  -- 1, 2, 99 (OTHER)
    vendor_name         VARCHAR(60) NOT NULL,
    vendor_abbreviation VARCHAR(10) NOT NULL,     -- CMT, VTS, OTHER
    vendor_status       VARCHAR(25) NOT NULL
);

-- 5. Dim_PaymentType
CREATE TABLE IF NOT EXISTS dds.dim_paymenttype (
    payment_type_key    SMALLINT PRIMARY KEY,     -- 1..7
    payment_type_id     SMALLINT NOT NULL UNIQUE, -- 0..6
    payment_type_name   VARCHAR(30) NOT NULL,
    payment_category    VARCHAR(20) NOT NULL,
    allows_tip          SMALLINT NOT NULL         -- 1 nếu Credit card, 0 các loại khác
);

-- 6. Dim_RateCode
CREATE TABLE IF NOT EXISTS dds.dim_ratecode (
    rate_code_key       SMALLINT PRIMARY KEY,     -- 1..7
    rate_code_id        SMALLINT NOT NULL UNIQUE, -- 1..6, 99
    rate_code_name      VARCHAR(50) NOT NULL,
    rate_type           VARCHAR(30) NOT NULL,
    description         VARCHAR(120)
);

-- 7. Dim_PassengerGroup
CREATE TABLE IF NOT EXISTS dds.dim_passengergroup (
    passenger_group_key   SMALLINT PRIMARY KEY,   -- 1..4
    passenger_count_range VARCHAR(30) NOT NULL UNIQUE,
    passenger_group_name  VARCHAR(30) NOT NULL,
    min_passenger         SMALLINT NOT NULL,
    max_passenger         SMALLINT NOT NULL,
    description           VARCHAR(120)
);

-- 8. Dim_DistanceBand
CREATE TABLE IF NOT EXISTS dds.dim_distanceband (
    distance_band_key   SMALLINT PRIMARY KEY,     -- 1..5
    distance_band_code  VARCHAR(20) NOT NULL UNIQUE,
    distance_band_name  VARCHAR(30) NOT NULL,
    min_distance_miles  DECIMAL(6,2) NOT NULL,
    max_distance_miles  DECIMAL(6,2) NOT NULL,
    trip_type           VARCHAR(40) NOT NULL
);

-- 9. Fact_TaxiTrip (1 PK + 11 FK + 1 Degenerate Dim + 13 Additive Measures)
CREATE TABLE IF NOT EXISTS dds.fact_taxitrip (
    trip_fact_key         BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    pickup_date_key       INTEGER NOT NULL REFERENCES dds.dim_date(date_key),
    dropoff_date_key      INTEGER NOT NULL REFERENCES dds.dim_date(date_key),
    pickup_time_key       INTEGER NOT NULL REFERENCES dds.dim_time(time_key),
    dropoff_time_key      INTEGER NOT NULL REFERENCES dds.dim_time(time_key),
    pickup_location_key   INTEGER NOT NULL REFERENCES dds.dim_location(location_key),
    dropoff_location_key  INTEGER NOT NULL REFERENCES dds.dim_location(location_key),
    vendor_key            SMALLINT NOT NULL REFERENCES dds.dim_vendor(vendor_key),
    payment_type_key      SMALLINT NOT NULL REFERENCES dds.dim_paymenttype(payment_type_key),
    rate_code_key         SMALLINT NOT NULL REFERENCES dds.dim_ratecode(rate_code_key),
    passenger_group_key   SMALLINT NOT NULL REFERENCES dds.dim_passengergroup(passenger_group_key),
    distance_band_key     SMALLINT NOT NULL REFERENCES dds.dim_distanceband(distance_band_key),
    store_and_fwd_flag    CHAR(1) NOT NULL,
    trip_count            SMALLINT NOT NULL DEFAULT 1,
    passenger_count       SMALLINT NOT NULL,
    trip_distance_miles   DECIMAL(8,2) NOT NULL,
    trip_duration_minutes DECIMAL(8,2) NOT NULL,
    fare_amount           DECIMAL(10,2) NOT NULL,
    extra_amount          DECIMAL(8,2) NOT NULL,
    mta_tax               DECIMAL(8,2) NOT NULL,
    tip_amount            DECIMAL(10,2) NOT NULL,
    tolls_amount          DECIMAL(8,2) NOT NULL,
    improvement_surcharge DECIMAL(8,2) NOT NULL,
    congestion_surcharge  DECIMAL(8,2) NOT NULL,
    airport_fee           DECIMAL(8,2) NOT NULL,
    total_amount          DECIMAL(10,2) NOT NULL,
    -- Cột kỹ thuật phục vụ Incremental & Lineage (dbt incremental)
    batch_id              VARCHAR(64) NOT NULL,
    source_file           VARCHAR(255) NOT NULL
);
