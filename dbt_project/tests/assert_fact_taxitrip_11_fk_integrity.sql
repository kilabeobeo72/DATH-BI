-- Singular Test 4: Kiểm tra tính toàn vẹn tuyệt đối của 11 khóa ngoại trong Fact_TaxiTrip
-- Ràng buộc: Mọi bản ghi trong Fact_TaxiTrip đều phải kết nối thành công tới 8 bảng Dimension.
-- Không được phép tồn tại bất kỳ khóa ngoại mồ côi (orphaned FK) hoặc giá trị NULL.
-- Nếu truy vấn trả về bất kỳ dòng nào, kiểm thử THẤT BẠI.

SELECT
    f.trip_id,
    f.batch_id,
    f.source_file,
    f.pickup_date_key,
    f.dropoff_date_key,
    f.pickup_time_key,
    f.dropoff_time_key,
    f.pickup_location_key,
    f.dropoff_location_key,
    f.vendor_key,
    f.payment_type_key,
    f.rate_code_key,
    f.passenger_group_key,
    f.distance_band_key
FROM {{ ref('fact_taxitrip') }} f
LEFT JOIN {{ source('dds', 'dim_date') }} d_pu ON f.pickup_date_key = d_pu.date_key
LEFT JOIN {{ source('dds', 'dim_date') }} d_do ON f.dropoff_date_key = d_do.date_key
LEFT JOIN {{ source('dds', 'dim_time') }} t_pu ON f.pickup_time_key = t_pu.time_key
LEFT JOIN {{ source('dds', 'dim_time') }} t_do ON f.dropoff_time_key = t_do.time_key
LEFT JOIN {{ source('dds', 'dim_location') }} l_pu ON f.pickup_location_key = l_pu.location_key
LEFT JOIN {{ source('dds', 'dim_location') }} l_do ON f.dropoff_location_key = l_do.location_key
LEFT JOIN {{ source('dds', 'dim_vendor') }} v ON f.vendor_key = v.vendor_key
LEFT JOIN {{ source('dds', 'dim_paymenttype') }} p ON f.payment_type_key = p.payment_type_key
LEFT JOIN {{ source('dds', 'dim_ratecode') }} r ON f.rate_code_key = r.rate_code_key
LEFT JOIN {{ source('dds', 'dim_passengergroup') }} pg ON f.passenger_group_key = pg.passenger_group_key
LEFT JOIN {{ source('dds', 'dim_distanceband') }} db ON f.distance_band_key = db.distance_band_key
WHERE d_pu.date_key IS NULL
   OR d_do.date_key IS NULL
   OR t_pu.time_key IS NULL
   OR t_do.time_key IS NULL
   OR l_pu.location_key IS NULL
   OR l_do.location_key IS NULL
   OR v.vendor_key IS NULL
   OR p.payment_type_key IS NULL
   OR r.rate_code_key IS NULL
   OR pg.passenger_group_key IS NULL
   OR db.distance_band_key IS NULL
