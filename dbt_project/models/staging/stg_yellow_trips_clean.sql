{{ config(materialized='view', schema='stage') }}

-- Làm sạch stage.stg_yellow_trips, gắn cờ lỗi và tra khóa các bảng dim.
-- Mỗi dòng stage ra đúng một dòng: is_valid = TRUE vào fact, FALSE vào quarantine.
-- error_reason là lỗi đầu tiên theo thứ tự ưu tiên, all_errors chứa toàn bộ lỗi.

WITH src AS (
    SELECT * FROM {{ source('stage', 'stg_yellow_trips') }}
),

norm AS (
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
        total_amount,  -- không COALESCE, NULL bị bắt ở NULL_REQUIRED
        COALESCE(congestion_surcharge, 0.00) AS congestion_surcharge,
        COALESCE(airport_fee, 0.00) AS airport_fee,
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 AS trip_duration_minutes,
        to_jsonb(src) AS raw_payload
    FROM src
),

speed AS (
    SELECT
        norm.*,
        CASE
            WHEN trip_duration_minutes > 0 THEN trip_distance / (trip_duration_minutes / 60.0)
        END AS implied_speed_mph
    FROM norm
),

keyed AS (
    SELECT
        s.*,
        d_pu.date_key              AS pickup_date_key,
        d_do.date_key              AS dropoff_date_key,
        t_pu.time_key              AS pickup_time_key,
        t_do.time_key              AS dropoff_time_key,
        l_pu.location_key          AS pickup_location_key,
        l_do.location_key          AS dropoff_location_key,
        v.vendor_key               AS vendor_key,
        p.payment_type_key         AS payment_type_key,
        r.rate_code_key            AS rate_code_key,
        pg.passenger_group_key     AS passenger_group_key,
        db.distance_band_key       AS distance_band_key
    FROM speed s
    LEFT JOIN {{ source('dds', 'dim_date') }} d_pu
           ON d_pu.date_key = CAST(TO_CHAR(s.tpep_pickup_datetime, 'YYYYMMDD') AS INTEGER)
    LEFT JOIN {{ source('dds', 'dim_date') }} d_do
           ON d_do.date_key = CAST(TO_CHAR(s.tpep_dropoff_datetime, 'YYYYMMDD') AS INTEGER)
    LEFT JOIN {{ source('dds', 'dim_time') }} t_pu
           ON t_pu.time_key = EXTRACT(HOUR FROM s.tpep_pickup_datetime)::INT * 100
                            + EXTRACT(MINUTE FROM s.tpep_pickup_datetime)::INT
    LEFT JOIN {{ source('dds', 'dim_time') }} t_do
           ON t_do.time_key = EXTRACT(HOUR FROM s.tpep_dropoff_datetime)::INT * 100
                            + EXTRACT(MINUTE FROM s.tpep_dropoff_datetime)::INT
    LEFT JOIN {{ source('dds', 'dim_location') }} l_pu ON l_pu.location_id = s.pu_location_id
    LEFT JOIN {{ source('dds', 'dim_location') }} l_do ON l_do.location_id = s.do_location_id
    -- vendor khác 1, 2 vào OTHER (vendor_id = 99 trong dim_vendor)
    LEFT JOIN {{ source('dds', 'dim_vendor') }} v
           ON v.vendor_id = CASE WHEN s.vendor_id IN (1, 2) THEN s.vendor_id ELSE 99 END
    LEFT JOIN {{ source('dds', 'dim_paymenttype') }} p ON p.payment_type_id = s.payment_type
    LEFT JOIN {{ source('dds', 'dim_ratecode') }} r ON r.rate_code_id = s.ratecode_id
    -- passenger_count = 0 vào nhóm 4 (chưa xác định)
    LEFT JOIN LATERAL (
        SELECT g.passenger_group_key
        FROM {{ source('dds', 'dim_passengergroup') }} g
        WHERE (s.passenger_count = 0 AND g.passenger_group_key = 4)
           OR (s.passenger_count > 0 AND g.passenger_group_key <> 4
               AND s.passenger_count BETWEEN g.min_passenger AND g.max_passenger)
        ORDER BY g.passenger_group_key
        LIMIT 1
    ) pg ON TRUE
    -- dải có cận dưới lớn nhất mà <= trip_distance
    LEFT JOIN LATERAL (
        SELECT b.distance_band_key
        FROM {{ source('dds', 'dim_distanceband') }} b
        WHERE s.trip_distance >= b.min_distance_miles
        ORDER BY b.min_distance_miles DESC
        LIMIT 1
    ) db ON TRUE
),

flagged AS (
    SELECT
        k.*,
        ARRAY_REMOVE(ARRAY[
            -- NULL ở cột bắt buộc
            CASE WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL
                   OR pu_location_id IS NULL OR do_location_id IS NULL
                   OR vendor_id IS NULL OR payment_type IS NULL
                   OR trip_distance IS NULL OR total_amount IS NULL
                 THEN 'NULL_REQUIRED: thiếu giá trị ở cột bắt buộc' END,
            -- pickup trong 2023-2024, dropoff đến hết 2025-01-01
            CASE WHEN tpep_pickup_datetime < TIMESTAMP '2023-01-01'
                   OR tpep_pickup_datetime >= TIMESTAMP '2025-01-01'
                   OR tpep_dropoff_datetime < TIMESTAMP '2023-01-01'
                   OR tpep_dropoff_datetime >= TIMESTAMP '2025-01-02'
                 THEN 'YEAR_OUT_OF_RANGE: ngày ngoài phạm vi 2023-2024' END,
            -- dropoff sau pickup, thời lượng 1-300 phút
            CASE WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
                   OR trip_duration_minutes < 1 OR trip_duration_minutes > 300
                 THEN 'DURATION_INVALID: thời lượng ngoài [1, 300] phút hoặc dropoff <= pickup' END,
            -- hành khách < 0 hoặc > 9 (0 được giữ)
            CASE WHEN passenger_count < 0 OR passenger_count > 9
                 THEN 'PASSENGER_INVALID: số hành khách < 0 hoặc > 9' END,
            -- quãng đường 0-150 dặm
            CASE WHEN trip_distance <= 0 OR trip_distance > 150
                 THEN 'DISTANCE_INVALID: quãng đường <= 0 hoặc > 150 dặm' END,
            -- tiền thành phần âm
            CASE WHEN fare_amount < 0 OR extra < 0 OR mta_tax < 0 OR tip_amount < 0
                   OR tolls_amount < 0 OR improvement_surcharge < 0
                   OR congestion_surcharge < 0 OR airport_fee < 0
                 THEN 'NEGATIVE_AMOUNT: có khoản tiền thành phần < 0' END,
            -- total_amount >= 0 nếu payment_type 3, 4, còn lại phải > 0
            CASE WHEN (payment_type IN (3, 4) AND total_amount < 0)
                   OR (payment_type NOT IN (3, 4) AND total_amount <= 0)
                 THEN 'TOTAL_AMOUNT_INVALID: total_amount không hợp lệ theo payment_type' END,
            -- vận tốc ngoài [0.5, 85] mph
            CASE WHEN implied_speed_mph < 0.5 OR implied_speed_mph > 85
                 THEN 'SPEED_INVALID: vận tốc ngoài [0.5, 85] mph' END,
            -- không tra được khóa dim
            CASE WHEN pickup_date_key IS NULL OR dropoff_date_key IS NULL
                 THEN 'FK_DATE_NOT_FOUND: không có trong dim_date' END,
            CASE WHEN pickup_time_key IS NULL OR dropoff_time_key IS NULL
                 THEN 'FK_TIME_NOT_FOUND: không có trong dim_time' END,
            CASE WHEN pu_location_id IS NOT NULL AND pickup_location_key IS NULL
                 THEN 'FK_PU_LOCATION_NOT_FOUND: pu_location_id không có trong dim_location' END,
            CASE WHEN do_location_id IS NOT NULL AND dropoff_location_key IS NULL
                 THEN 'FK_DO_LOCATION_NOT_FOUND: do_location_id không có trong dim_location' END,
            CASE WHEN vendor_id IS NOT NULL AND vendor_key IS NULL
                 THEN 'FK_VENDOR_NOT_FOUND: dim_vendor thiếu vendor 1, 2 hoặc 99 (OTHER)' END,
            CASE WHEN payment_type IS NOT NULL AND payment_type_key IS NULL
                 THEN 'FK_PAYMENT_NOT_FOUND: payment_type không có trong dim_paymenttype' END,
            CASE WHEN rate_code_key IS NULL
                 THEN 'FK_RATECODE_NOT_FOUND: ratecode_id không có trong dim_ratecode' END,
            CASE WHEN passenger_group_key IS NULL AND passenger_count BETWEEN 0 AND 9
                 THEN 'FK_PASSENGER_GROUP_NOT_FOUND: không có nhóm hành khách phù hợp' END,
            CASE WHEN distance_band_key IS NULL AND trip_distance > 0
                 THEN 'FK_DISTANCE_BAND_NOT_FOUND: không có dải quãng đường phù hợp' END
        ], NULL) AS all_errors
    FROM keyed k
)

SELECT
    batch_id,
    source_file,
    ingested_at,
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    trip_duration_minutes,
    implied_speed_mph,
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
    all_errors,
    (CARDINALITY(all_errors) = 0) AS is_valid,
    all_errors[1] AS error_reason,
    raw_payload
FROM flagged
