from datetime import datetime
import os
import uuid

if 'data_loader' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        def data_loader(fn):
            return fn

from mage_project.custom.db_utils import get_db_cursor


@data_loader
def load_watermark_and_cet(*args, **kwargs):
    """
    Bước 1 (Thành viên 2 - Thạch Ý):
    - Lấy CET (Current Extraction Time) NGAY TẠI THỜI ĐIỂM BẮT ĐẦU pipeline.
    - Tuyệt đối không lấy NOW() ở cuối pipeline để tránh mất mát dữ liệu phát sinh trong khi chạy.
    - Xác định chế độ chạy: Full Load ('full') hoặc Incremental Load ('incremental').
    - Nếu 'full': đặt mốc LSET = '1900-01-01 00:00:00'.
    - Nếu 'incremental': đọc LSET từ ctrl.etl_watermark (last_successful_extraction_time).
    - Khởi tạo batch_id duy nhất và ghi nhận trạng thái 'RUNNING' vào ctrl.etl_run_log & ctrl.etl_watermark.
    - Truyền context (pipeline_name, load_mode, batch_id, run_id, lset, cet) cho các block tiếp theo.
    """
    cet = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_mode = str(kwargs.get("load_mode", os.getenv("LOAD_MODE", "incremental"))).lower().strip()
    if load_mode not in ("full", "incremental"):
        load_mode = "incremental"

    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    pipeline_name = "etl_taxi_trips"
    run_id = None

    with get_db_cursor(commit=True) as (conn, cursor):
        if load_mode == "full":
            lset = "1900-01-01 00:00:00"
        else:
            cursor.execute(
                """
                SELECT COALESCE(last_successful_extraction_time, lset, TIMESTAMP '1900-01-01 00:00:00')
                FROM ctrl.etl_watermark
                WHERE pipeline_name = %s;
                """,
                (pipeline_name,),
            )
            row = cursor.fetchone()
            if row and row[0]:
                lset = row[0].strftime("%Y-%m-%d %H:%M:%S")
            else:
                lset = "1900-01-01 00:00:00"

        # Ghi log bắt đầu vào ctrl.etl_run_log
        cursor.execute(
            """
            INSERT INTO ctrl.etl_run_log (
                pipeline_name, batch_id, load_mode, run_mode,
                lset_used, cet_captured, status, started_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, 'RUNNING', CURRENT_TIMESTAMP
            ) RETURNING run_id;
            """,
            (pipeline_name, batch_id, load_mode, load_mode, lset, cet),
        )
        run_row = cursor.fetchone()
        if run_row:
            run_id = run_row[0]

        # Đánh dấu watermark sang trạng thái RUNNING
        cursor.execute(
            """
            INSERT INTO ctrl.etl_watermark (
                pipeline_name, last_successful_extraction_time, last_batch_id,
                last_status, last_run_mode, updated_at
            ) VALUES (
                %s, %s, %s, 'RUNNING', %s, CURRENT_TIMESTAMP
            )
            ON CONFLICT (pipeline_name) DO UPDATE
            SET last_batch_id = EXCLUDED.last_batch_id,
                last_status = 'RUNNING',
                last_run_mode = EXCLUDED.last_run_mode,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (pipeline_name, lset, batch_id, load_mode),
        )

    print(f"[{pipeline_name}] Started batch={batch_id}, mode={load_mode}, LSET={lset}, CET={cet}, run_id={run_id}")

    return {
        "pipeline_name": pipeline_name,
        "load_mode": load_mode,
        "batch_id": batch_id,
        "run_id": run_id,
        "lset": str(lset),
        "cet": str(cet),
    }
