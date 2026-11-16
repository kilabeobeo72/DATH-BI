{{ config(
    materialized='incremental',
    schema='ctrl',
    incremental_strategy='delete+insert',
    unique_key='batch_id',
    full_refresh=false,
    on_schema_change='append_new_columns',
    pre_hook="{% if flags.FULL_REFRESH and load_relation(this) is not none %}TRUNCATE TABLE {{ this }} RESTART IDENTITY{% endif %}"
) }}

-- Bản ghi is_valid = FALSE. Chạy lại cùng batch_id thì xóa rồi nạp lại.
-- Full refresh dùng TRUNCATE để giữ nguyên bảng tạo từ sql/init.
SELECT
    batch_id,
    source_file,
    LEFT(error_reason, 200) AS error_reason,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pu_location_id,
    do_location_id,
    (raw_payload ->> 'passenger_count')::DOUBLE PRECISION AS passenger_count,
    trip_distance,
    total_amount,
    raw_payload || JSONB_BUILD_OBJECT('all_errors', TO_JSONB(all_errors)) AS raw_payload
FROM {{ ref('stg_yellow_trips_clean') }}
WHERE is_valid = FALSE
