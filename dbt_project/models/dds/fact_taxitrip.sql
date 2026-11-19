{{ config(
    materialized='incremental',
    schema='dds',
    incremental_strategy='delete+insert',
    unique_key='batch_id',
    full_refresh=false,
    on_schema_change='append_new_columns',
    pre_hook="{% if flags.FULL_REFRESH and load_relation(this) is not none %}TRUNCATE TABLE {{ this }} RESTART IDENTITY{% endif %}"
) }}

-- Bản ghi hợp lệ (is_valid = TRUE), khóa dim đã tra ở stg_yellow_trips_clean.
-- Chạy lại cùng batch_id thì xóa rồi nạp lại. Full refresh dùng TRUNCATE để giữ identity và FK.
SELECT
    pickup_date_key,
    dropoff_date_key,
    pickup_time_key,
    dropoff_time_key,
    pickup_location_key,
    dropoff_location_key,
    vendor_key,
    payment_type_key,
    rate_code_key,
    passenger_group_key,
    distance_band_key,
    store_and_fwd_flag::CHAR(1)                       AS store_and_fwd_flag,
    1::SMALLINT                                       AS trip_count,
    passenger_count::SMALLINT                         AS passenger_count,
    ROUND(trip_distance::NUMERIC, 2)                  AS trip_distance_miles,
    ROUND(trip_duration_minutes::NUMERIC, 2)          AS trip_duration_minutes,
    ROUND(fare_amount::NUMERIC, 2)                    AS fare_amount,
    ROUND(extra::NUMERIC, 2)                          AS extra_amount,
    ROUND(mta_tax::NUMERIC, 2)                        AS mta_tax,
    ROUND(tip_amount::NUMERIC, 2)                     AS tip_amount,
    ROUND(tolls_amount::NUMERIC, 2)                   AS tolls_amount,
    ROUND(improvement_surcharge::NUMERIC, 2)          AS improvement_surcharge,
    ROUND(congestion_surcharge::NUMERIC, 2)           AS congestion_surcharge,
    ROUND(airport_fee::NUMERIC, 2)                    AS airport_fee,
    ROUND(total_amount::NUMERIC, 2)                   AS total_amount,
    batch_id,
    source_file
FROM {{ ref('stg_yellow_trips_clean') }}
WHERE is_valid = TRUE
