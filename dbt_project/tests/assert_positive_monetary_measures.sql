-- Singular Test 2: Kiểm tra các độ đo tiền tệ trong bảng Fact_TaxiTrip
-- Ràng buộc: Mọi khoản phí thành phần (fare, extra, mta_tax, tip, tolls, improvement, congestion, airport_fee)
-- đều phải >= 0.00. Tổng tiền total_amount phải >= 0.00.
-- Nếu truy vấn trả về bất kỳ bản ghi nào, kiểm thử THẤT BẠI.

SELECT
    trip_id,
    batch_id,
    source_file,
    fare_amount,
    extra_amount,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount
FROM {{ ref('fact_taxitrip') }}
WHERE fare_amount < 0
   OR extra_amount < 0
   OR mta_tax < 0
   OR tip_amount < 0
   OR tolls_amount < 0
   OR improvement_surcharge < 0
   OR congestion_surcharge < 0
   OR airport_fee < 0
   OR total_amount < 0
