{{ config(materialized='incremental', schema='ctrl') }}

-- TODO (Thành viên 3 - Dương):
-- Lọc các bản ghi is_valid = FALSE từ {{ ref('stg_yellow_trips_clean') }}
-- để ghi vào ctrl.quarantine_log (hỗ trợ cả full-refresh và incremental theo batch_id)
SELECT
    batch_id,
    source_file,
    'TODO_ERROR_REASON'::VARCHAR(200) AS error_reason,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pu_location_id,
    do_location_id,
    passenger_count::DOUBLE PRECISION AS passenger_count,
    trip_distance,
    total_amount,
    CURRENT_TIMESTAMP AS quarantined_at
FROM {{ ref('stg_yellow_trips_clean') }}
WHERE 1 = 0 -- TODO: thay bằng điều kiện is_valid = FALSE
