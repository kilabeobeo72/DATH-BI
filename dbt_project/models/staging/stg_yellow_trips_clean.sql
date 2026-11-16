{{ config(materialized='view', schema='stage') }}

-- TODO (Thành viên 3 - Dương):
-- Chuẩn hóa và gắn cờ kiểm tra 11 nhóm dị thường từ {{ source('stage', 'stg_yellow_trips') }}
-- Xuất ra các cột đã tính sẵn: trip_duration_minutes, implied_speed_mph, is_valid, error_reason
SELECT
    batch_id,
    source_file,
    ingested_at,
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    COALESCE(passenger_count, 0) AS passenger_count,
    trip_distance,
    COALESCE(ratecode_id, 99) AS ratecode_id,
    COALESCE(NULLIF(TRIM(store_and_fwd_flag), ''), 'N') AS store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    COALESCE(fare_amount, 0.00) AS fare_amount,
    COALESCE(extra, 0.00) AS extra,
    COALESCE(mta_tax, 0.00) AS mta_tax,
    COALESCE(tip_amount, 0.00) AS tip_amount,
    COALESCE(tolls_amount, 0.00) AS tolls_amount,
    COALESCE(improvement_surcharge, 0.00) AS improvement_surcharge,
    COALESCE(total_amount, 0.00) AS total_amount,
    COALESCE(congestion_surcharge, 0.00) AS congestion_surcharge,
    COALESCE(airport_fee, 0.00) AS airport_fee
FROM {{ source('stage', 'stg_yellow_trips') }}
