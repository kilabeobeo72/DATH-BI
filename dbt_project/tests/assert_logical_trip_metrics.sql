-- Singular Test 3: Kiểm tra tính logic của các độ đo vận hành trong Fact_TaxiTrip
-- Ràng buộc:
-- 1. Thời gian di chuyển trip_duration_minutes phải trong khoảng [1, 300] phút.
-- 2. Quãng đường di chuyển trip_distance_miles phải trong khoảng (0, 150] dặm.
-- 3. Số lượng hành khách passenger_count phải trong khoảng [0, 9].
-- 4. Vận tốc ước tính (miles / (minutes / 60)) phải trong khoảng [0.5, 85.0] mph.
-- Nếu truy vấn trả về bất kỳ dòng nào, kiểm thử THẤT BẠI.

WITH speed_calc AS (
    SELECT
        trip_fact_key,
        batch_id,
        source_file,
        trip_distance_miles,
        trip_duration_minutes,
        passenger_count,
        CASE
            WHEN trip_duration_minutes > 0
            THEN trip_distance_miles / (trip_duration_minutes / 60.0)
            ELSE 0
        END AS calculated_speed_mph
    FROM {{ ref('fact_taxitrip') }}
)

SELECT
    trip_fact_key,
    batch_id,
    source_file,
    trip_distance_miles,
    trip_duration_minutes,
    passenger_count,
    calculated_speed_mph
FROM speed_calc
WHERE trip_duration_minutes < 1
   OR trip_duration_minutes > 300
   OR trip_distance_miles <= 0
   OR trip_distance_miles > 150
   OR passenger_count < 0
   OR passenger_count > 9
   OR calculated_speed_mph < 0.5
   OR calculated_speed_mph > 85.0
