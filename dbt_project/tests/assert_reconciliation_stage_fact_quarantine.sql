-- Singular Test 1: Đối soát bảo toàn số dòng giữa Stage, Fact và Quarantine
-- Nguyên tắc: Với mỗi batch_id, số dòng nạp vào stage.stg_yellow_trips
-- BẮT BUỘC PHẢI BẰNG số dòng vào dds.fact_taxitrip + ctrl.quarantine_log.
-- Nếu truy vấn trả về bất kỳ dòng nào, kiểm thử THẤT BẠI.

WITH stage_counts AS (
    SELECT
        batch_id,
        COUNT(*) AS total_stage_rows
    FROM {{ source('stage', 'stg_yellow_trips') }}
    GROUP BY batch_id
),

fact_counts AS (
    SELECT
        batch_id,
        COUNT(*) AS total_fact_rows
    FROM {{ ref('fact_taxitrip') }}
    GROUP BY batch_id
),

quarantine_counts AS (
    SELECT
        batch_id,
        COUNT(*) AS total_quarantine_rows
    FROM {{ ref('quarantine_log') }}
    GROUP BY batch_id
),

reconciliation AS (
    SELECT
        s.batch_id,
        s.total_stage_rows,
        COALESCE(f.total_fact_rows, 0) AS total_fact_rows,
        COALESCE(q.total_quarantine_rows, 0) AS total_quarantine_rows,
        (COALESCE(f.total_fact_rows, 0) + COALESCE(q.total_quarantine_rows, 0)) AS total_processed_rows
    FROM stage_counts s
    LEFT JOIN fact_counts f ON s.batch_id = f.batch_id
    LEFT JOIN quarantine_counts q ON s.batch_id = q.batch_id
)

SELECT
    batch_id,
    total_stage_rows,
    total_fact_rows,
    total_quarantine_rows,
    total_processed_rows,
    (total_stage_rows - total_processed_rows) AS diff_count
FROM reconciliation
WHERE total_stage_rows <> total_processed_rows
