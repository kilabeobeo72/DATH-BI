-- ====================================================================
-- BỘ TRUY VẤN ĐỐI SOÁT & KIỂM CHỨNG TOÀN DIỆN PIPELINE ETL (GIAI ĐOẠN 2)
-- Người phụ trách: Phạm Ngọc Thanh - MSSV: 23120087 (Thành viên 4)
-- ====================================================================

-- ====================================================================
-- PHẦN 1: KIỂM TRA SỐ LƯỢNG BẢN GHI TẠI 8 BẢNG DIMENSION (DDS)
-- ====================================================================
SELECT 'dim_date' AS table_name, COUNT(*) AS row_cnt FROM dds.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM dds.dim_time
UNION ALL SELECT 'dim_location', COUNT(*) FROM dds.dim_location
UNION ALL SELECT 'dim_vendor', COUNT(*) FROM dds.dim_vendor
UNION ALL SELECT 'dim_paymenttype', COUNT(*) FROM dds.dim_paymenttype
UNION ALL SELECT 'dim_ratecode', COUNT(*) FROM dds.dim_ratecode
UNION ALL SELECT 'dim_passengergroup', COUNT(*) FROM dds.dim_passengergroup
UNION ALL SELECT 'dim_distanceband', COUNT(*) FROM dds.dim_distanceband;
-- Kỳ vọng:
-- dim_date: 732 dòng (2023-01-01 đến 2025-01-01)
-- dim_time: 1440 dòng (1440 phút trong ngày)
-- dim_location: 265 dòng (khu vực 1..265)
-- dim_vendor: >= 3 dòng (1: CMT, 2: VTS, 99: OTHER)
-- dim_paymenttype: 6 dòng (1..6)
-- dim_ratecode: 7 dòng (1..6, 99)
-- dim_passengergroup: 4 dòng (1: Đi một mình, 2: Nhóm 2-4, 3: Nhóm 5+, 4: Chưa xác định)
-- dim_distanceband: 5 dòng (Rất ngắn, Ngắn, Trung bình, Dài, Rất dài)

-- ====================================================================
-- PHẦN 2: KIỂM TRA WATERMARK & TRẠNG THÁI TIẾN TRÌNH ETL (CTRL)
-- ====================================================================
-- 2.1. Trạng thái Watermark hiện tại (LSET và CET)
SELECT
    pipeline_name,
    lset,
    cet,
    last_run_mode,
    last_status,
    updated_at
FROM ctrl.etl_watermark;

-- 2.2. Lịch sử thực thi các lần chạy (Run Log)
SELECT
    run_id,
    batch_id,
    run_mode,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds,
    records_extracted,
    records_fact_inserted,
    records_quarantined
FROM ctrl.etl_run_log
ORDER BY run_id DESC
LIMIT 20;

-- 2.3. Danh sách các file Parquet đã nạp thành công
SELECT
    status,
    COUNT(*) AS total_files,
    SUM(row_count) AS total_source_rows
FROM ctrl.processed_files
GROUP BY status;

-- ====================================================================
-- PHẦN 3: ĐỐI SOÁT CÂN BẰNG BẢO TOÀN DỮ LIỆU: RAW = FACT + QUARANTINE
-- ====================================================================
-- 3.1. Đối soát chi tiết theo từng tệp dữ liệu nguồn (source_file)
WITH fact_stat AS (
    SELECT
        source_file,
        COUNT(*) AS fact_rows
    FROM dds.fact_taxitrip
    GROUP BY source_file
),
quarantine_stat AS (
    SELECT
        source_file,
        COUNT(*) AS quarantine_rows
    FROM ctrl.quarantine_log
    GROUP BY source_file
),
file_stat AS (
    SELECT
        file_name AS source_file,
        row_count AS raw_rows
    FROM ctrl.processed_files
)
SELECT
    COALESCE(fl.source_file, f.source_file, q.source_file) AS source_file,
    COALESCE(fl.raw_rows, 0) AS raw_rows,
    COALESCE(f.fact_rows, 0) AS fact_rows,
    COALESCE(q.quarantine_rows, 0) AS quarantine_rows,
    (COALESCE(f.fact_rows, 0) + COALESCE(q.quarantine_rows, 0)) AS total_processed,
    (COALESCE(fl.raw_rows, 0) - (COALESCE(f.fact_rows, 0) + COALESCE(q.quarantine_rows, 0))) AS discrepancy_diff,
    ROUND(COALESCE(f.fact_rows, 0)::NUMERIC / NULLIF(fl.raw_rows, 0) * 100, 2) AS clean_rate_pct,
    ROUND(COALESCE(q.quarantine_rows, 0)::NUMERIC / NULLIF(fl.raw_rows, 0) * 100, 2) AS quarantine_rate_pct
FROM file_stat fl
FULL OUTER JOIN fact_stat f ON fl.source_file = f.source_file
FULL OUTER JOIN quarantine_stat q ON COALESCE(fl.source_file, f.source_file) = q.source_file
ORDER BY source_file;

-- 3.2. Bảng tổng hợp các lý do bản ghi bị đẩy vào Quarantine
SELECT
    error_reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM ctrl.quarantine_log) * 100, 2) AS pct_of_rejected
FROM ctrl.quarantine_log
GROUP BY error_reason
ORDER BY rejected_count DESC;

-- ====================================================================
-- PHẦN 4: KIỂM TRA TÍNH TOÀN VẸN 11 KHÓA NGOẠI (FK INTEGRITY)
-- ====================================================================
-- Kỳ vọng: Toàn bộ đều trả về 0 (Không có khóa ngoại mồ côi hoặc NULL)
SELECT
    COUNT(*) AS total_fact_records,
    COUNT(*) FILTER (WHERE pickup_date_key IS NULL) AS null_pu_date,
    COUNT(*) FILTER (WHERE dropoff_date_key IS NULL) AS null_do_date,
    COUNT(*) FILTER (WHERE pickup_time_key IS NULL) AS null_pu_time,
    COUNT(*) FILTER (WHERE dropoff_time_key IS NULL) AS null_do_time,
    COUNT(*) FILTER (WHERE pickup_location_key IS NULL) AS null_pu_loc,
    COUNT(*) FILTER (WHERE dropoff_location_key IS NULL) AS null_do_loc,
    COUNT(*) FILTER (WHERE vendor_key IS NULL) AS null_vendor,
    COUNT(*) FILTER (WHERE payment_type_key IS NULL) AS null_payment,
    COUNT(*) FILTER (WHERE rate_code_key IS NULL) AS null_ratecode,
    COUNT(*) FILTER (WHERE passenger_group_key IS NULL) AS null_pass_group,
    COUNT(*) FILTER (WHERE distance_band_key IS NULL) AS null_dist_band
FROM dds.fact_taxitrip;

-- ====================================================================
-- PHẦN 5: BỘ TRUY VẤN KIỂM CHỨNG NGHIỆP VỤ (CB1–CB8 & MR1–MR5)
-- ====================================================================

-- --------------------------------------------------------------------
-- CB1: Thống kê tổng số chuyến đi và tổng doanh thu theo từng tháng/năm
-- --------------------------------------------------------------------
SELECT
    d.year_number,
    d.month_number,
    d.month_name,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance_miles
FROM dds.fact_taxitrip f
JOIN dds.dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.year_number, d.month_number, d.month_name
ORDER BY d.year_number, d.month_number;

-- --------------------------------------------------------------------
-- CB2: Thống kê số chuyến và doanh thu theo ngày trong tuần (Thứ 2 - CN)
-- --------------------------------------------------------------------
SELECT
    d.day_number_of_week,
    d.day_name_of_week,
    CASE WHEN d.is_weekend = 1 THEN 'Cuối tuần' ELSE 'Ngày thường' END AS day_type,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_trip_amount
FROM dds.fact_taxitrip f
JOIN dds.dim_date d ON f.pickup_date_key = d.date_key
GROUP BY d.day_number_of_week, d.day_name_of_week, d.is_weekend
ORDER BY d.day_number_of_week;

-- --------------------------------------------------------------------
-- CB3: Thống kê chuyến đi và doanh thu theo khung giờ trong ngày
-- --------------------------------------------------------------------
SELECT
    t.peak_hour_category,
    t.time_of_day,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.trip_duration_minutes), 2) AS avg_duration_minutes,
    ROUND(AVG(f.congestion_surcharge), 2) AS avg_congestion_surcharge
FROM dds.fact_taxitrip f
JOIN dds.dim_time t ON f.pickup_time_key = t.time_key
GROUP BY t.peak_hour_category, t.time_of_day
ORDER BY total_trips DESC;

-- --------------------------------------------------------------------
-- CB4: Thống kê số chuyến, doanh thu theo từng Quận đón (Pickup Borough)
-- --------------------------------------------------------------------
SELECT
    loc.borough AS pickup_borough,
    loc.service_zone,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.fare_amount), 2) AS avg_fare,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance
FROM dds.fact_taxitrip f
JOIN dds.dim_location loc ON f.pickup_location_key = loc.location_key
GROUP BY loc.borough, loc.service_zone
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------
-- CB5: Phân tích số chuyến và doanh thu theo nhóm khách & dải cự ly
-- --------------------------------------------------------------------
SELECT
    pg.passenger_group_name,
    db.distance_band_name,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance,
    ROUND(AVG(f.trip_duration_minutes), 2) AS avg_duration
FROM dds.fact_taxitrip f
JOIN dds.dim_passengergroup pg ON f.passenger_group_key = pg.passenger_group_key
JOIN dds.dim_distanceband db ON f.distance_band_key = db.distance_band_key
GROUP BY pg.passenger_group_name, db.distance_band_name
ORDER BY pg.passenger_group_name, total_trips DESC;

-- --------------------------------------------------------------------
-- CB6: Cơ cấu hình thức thanh toán & tỷ lệ tiền boa trung bình
-- --------------------------------------------------------------------
SELECT
    p.payment_type_name,
    p.payment_category,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.tip_amount) AS total_tips,
    ROUND(SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0) * 100, 2) AS avg_tip_rate_pct
FROM dds.fact_taxitrip f
JOIN dds.dim_paymenttype p ON f.payment_type_key = p.payment_type_key
GROUP BY p.payment_type_name, p.payment_category
ORDER BY total_trips DESC;

-- --------------------------------------------------------------------
-- CB7: Thống kê số chuyến và doanh thu theo biểu cước (Rate Code)
-- --------------------------------------------------------------------
SELECT
    r.rate_code_name,
    r.rate_type,
    COUNT(*) AS total_trips,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_fare_per_trip
FROM dds.fact_taxitrip f
JOIN dds.dim_ratecode r ON f.rate_code_key = r.rate_code_key
GROUP BY r.rate_code_name, r.rate_type
ORDER BY total_trips DESC;

-- --------------------------------------------------------------------
-- CB8: Thị phần số chuyến và doanh thu theo nhà cung cấp thiết bị
-- --------------------------------------------------------------------
SELECT
    v.vendor_name,
    v.vendor_abbreviation,
    COUNT(*) AS total_trips,
    ROUND(COUNT(*)::NUMERIC / (SELECT COUNT(*) FROM dds.fact_taxitrip) * 100, 2) AS trip_market_share_pct,
    SUM(f.total_amount) AS total_revenue,
    ROUND(SUM(f.total_amount)::NUMERIC / (SELECT SUM(total_amount) FROM dds.fact_taxitrip) * 100, 2) AS rev_market_share_pct
FROM dds.fact_taxitrip f
JOIN dds.dim_vendor v ON f.vendor_key = v.vendor_key
GROUP BY v.vendor_name, v.vendor_abbreviation
ORDER BY total_trips DESC;

-- --------------------------------------------------------------------
-- MR1: So sánh tăng trưởng doanh thu & số chuyến giữa Tháng 5 và Tháng 6
-- --------------------------------------------------------------------
WITH monthly_metrics AS (
    SELECT
        d.year_number,
        d.month_number,
        d.month_name,
        COUNT(*) AS total_trips,
        SUM(f.total_amount) AS total_revenue
    FROM dds.fact_taxitrip f
    JOIN dds.dim_date d ON f.pickup_date_key = d.date_key
    WHERE d.month_number IN (5, 6)
    GROUP BY d.year_number, d.month_number, d.month_name
)
SELECT
    m5.year_number,
    m5.total_trips AS may_trips,
    m6.total_trips AS june_trips,
    (m6.total_trips - m5.total_trips) AS trip_change,
    ROUND((m6.total_trips - m5.total_trips)::NUMERIC / m5.total_trips * 100, 2) AS trip_growth_pct,
    m5.total_revenue AS may_revenue,
    m6.total_revenue AS june_revenue,
    (m6.total_revenue - m5.total_revenue) AS revenue_change,
    ROUND((m6.total_revenue - m5.total_revenue)::NUMERIC / m5.total_revenue * 100, 2) AS revenue_growth_pct
FROM monthly_metrics m5
JOIN monthly_metrics m6 ON m5.year_number = m6.year_number AND m5.month_number = 5 AND m6.month_number = 6
ORDER BY m5.year_number;

-- --------------------------------------------------------------------
-- MR2: Vận tốc di chuyển trung bình theo khung giờ & quận đón
-- --------------------------------------------------------------------
SELECT
    loc.borough AS pickup_borough,
    t.peak_hour_category,
    COUNT(*) AS total_trips,
    ROUND(AVG(f.trip_distance_miles / NULLIF(f.trip_duration_minutes / 60.0, 0)), 2) AS avg_speed_mph,
    ROUND(AVG(f.trip_duration_minutes), 2) AS avg_duration_minutes
FROM dds.fact_taxitrip f
JOIN dds.dim_location loc ON f.pickup_location_key = loc.location_key
JOIN dds.dim_time t ON f.pickup_time_key = t.time_key
GROUP BY loc.borough, t.peak_hour_category
ORDER BY loc.borough, avg_speed_mph ASC;

-- --------------------------------------------------------------------
-- MR3: Top 10 tuyến đường (Cặp Zone đón -> trả) có doanh thu cao nhất
-- --------------------------------------------------------------------
SELECT
    pu.zone_name AS pickup_zone,
    pu.borough AS pickup_borough,
    do_loc.zone_name AS dropoff_zone,
    do_loc.borough AS dropoff_borough,
    COUNT(*) AS trip_count,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_fare,
    ROUND(AVG(f.trip_distance_miles), 2) AS avg_distance
FROM dds.fact_taxitrip f
JOIN dds.dim_location pu ON f.pickup_location_key = pu.location_key
JOIN dds.dim_location do_loc ON f.dropoff_location_key = do_loc.location_key
GROUP BY pu.zone_name, pu.borough, do_loc.zone_name, do_loc.borough
ORDER BY total_revenue DESC
LIMIT 10;

-- --------------------------------------------------------------------
-- MR4: Tỷ lệ tiền boa theo khung giờ & quận (chỉ xét Thẻ tín dụng)
-- --------------------------------------------------------------------
SELECT
    loc.borough AS pickup_borough,
    t.time_of_day,
    COUNT(*) AS card_trips,
    SUM(f.fare_amount) AS total_base_fare,
    SUM(f.tip_amount) AS total_tips,
    ROUND(SUM(f.tip_amount) / NULLIF(SUM(f.fare_amount), 0) * 100, 2) AS avg_tip_percentage
FROM dds.fact_taxitrip f
JOIN dds.dim_paymenttype p ON f.payment_type_key = p.payment_type_key
JOIN dds.dim_location loc ON f.pickup_location_key = loc.location_key
JOIN dds.dim_time t ON f.pickup_time_key = t.time_key
WHERE p.payment_type_id = 1  -- Chỉ xét thẻ tín dụng có ghi nhận tip qua đồng hồ
GROUP BY loc.borough, t.time_of_day
ORDER BY avg_tip_percentage DESC;

-- --------------------------------------------------------------------
-- MR5: So sánh cùng kỳ (Year-over-Year) giữa 2023 và 2024 theo Quý
-- --------------------------------------------------------------------
WITH quarterly_data AS (
    SELECT
        d.year_number,
        d.quarter_name,
        COUNT(*) AS total_trips,
        SUM(f.total_amount) AS total_revenue,
        SUM(f.trip_distance_miles) AS total_miles,
        ROUND(SUM(f.total_amount) / NULLIF(SUM(f.trip_distance_miles), 0), 2) AS rev_per_mile
    FROM dds.fact_taxitrip f
    JOIN dds.dim_date d ON f.pickup_date_key = d.date_key
    GROUP BY d.year_number, d.quarter_name
)
SELECT
    q23.quarter_name,
    q23.total_trips AS trips_2023,
    q24.total_trips AS trips_2024,
    ROUND((q24.total_trips - q23.total_trips)::NUMERIC / q23.total_trips * 100, 2) AS trips_growth_yoy_pct,
    q23.total_revenue AS rev_2023,
    q24.total_revenue AS rev_2024,
    ROUND((q24.total_revenue - q23.total_revenue)::NUMERIC / q23.total_revenue * 100, 2) AS rev_growth_yoy_pct,
    q23.rev_per_mile AS rev_per_mile_2023,
    q24.rev_per_mile AS rev_per_mile_2024
FROM quarterly_data q23
JOIN quarterly_data q24 ON q23.quarter_name = q24.quarter_name AND q23.year_number = 2023 AND q24.year_number = 2024
ORDER BY q23.quarter_name;
