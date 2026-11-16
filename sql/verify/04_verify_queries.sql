-- ==========================================================
-- Template kiểm chứng đối soát & Kiểm thử ETL (Thành viên 4)
-- ==========================================================

-- 1. Kiểm tra số lượng bản ghi ở 8 bảng Dim
SELECT 'dim_date' AS table_name, COUNT(*) AS row_cnt FROM dds.dim_date
UNION ALL SELECT 'dim_time', COUNT(*) FROM dds.dim_time
UNION ALL SELECT 'dim_location', COUNT(*) FROM dds.dim_location
UNION ALL SELECT 'dim_vendor', COUNT(*) FROM dds.dim_vendor
UNION ALL SELECT 'dim_paymenttype', COUNT(*) FROM dds.dim_paymenttype
UNION ALL SELECT 'dim_ratecode', COUNT(*) FROM dds.dim_ratecode
UNION ALL SELECT 'dim_passengergroup', COUNT(*) FROM dds.dim_passengergroup
UNION ALL SELECT 'dim_distanceband', COUNT(*) FROM dds.dim_distanceband;

-- 2. Kiểm tra trạng thái Watermark & lịch sử chạy Full / Incremental
SELECT * FROM ctrl.etl_watermark;
SELECT * FROM ctrl.etl_run_log ORDER BY run_id DESC LIMIT 20;
SELECT status, COUNT(*), SUM(row_count) FROM ctrl.processed_files GROUP BY status;

-- 3. Đối soát tỷ lệ Clean (Fact) vs Rejected (Quarantine) theo từng file
SELECT source_file, COUNT(*) AS fact_rows
FROM dds.fact_taxitrip
GROUP BY source_file
ORDER BY source_file;

SELECT source_file, error_reason, COUNT(*) AS quarantine_rows
FROM ctrl.quarantine_log
GROUP BY source_file, error_reason
ORDER BY source_file, quarantine_rows DESC;

-- TODO (Thành viên 4): Bổ sung query kiểm chứng CB1–CB8 và MR1–MR5 trên dữ liệu thật
