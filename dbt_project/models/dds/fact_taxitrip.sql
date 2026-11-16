{{ config(materialized='incremental', schema='dds') }}

-- TODO (Thành viên 3 - Dương):
-- Nạp dữ liệu hợp lệ (is_valid = TRUE) từ {{ ref('stg_yellow_trips_clean') }}
-- JOIN 8 bảng Dim (Role-Playing Date/Time/Location) -> map 11 FK & tính 13 độ đo cơ sở
SELECT
    CAST(TO_CHAR(tpep_pickup_datetime, 'YYYYMMDD') AS INTEGER) AS pickup_date_key,
    CAST(TO_CHAR(tpep_dropoff_datetime, 'YYYYMMDD') AS INTEGER) AS dropoff_date_key,
    batch_id,
    source_file
FROM {{ ref('stg_yellow_trips_clean') }}
WHERE 1 = 0 -- TODO: hoàn thiện JOIN 8 bảng Dim và bộ lọc hợp lệ
