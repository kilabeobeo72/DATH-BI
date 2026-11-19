import io
import os
import pyarrow.parquet as pq

if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(fn):
            return fn

from mage_project.custom.db_utils import get_pg_connection
from mage_project.transformers.prepare_staging_batch import (
    STAGING_COLUMNS_ORDER,
    normalize_dataframe_batch,
)

CHUNK_SIZE = 100_000


@data_exporter
def export_to_staging(staged_payload, *args, **kwargs):
    """
    Bước 4 (Thành viên 2 - Thạch Ý):
    - Áp dụng chiến lược Truncate & Reload cho tầng Staging: TRUNCATE TABLE stage.stg_yellow_trips.
    - Đọc từng file Parquet theo batch nhỏ (iter_batches với pyarrow) để tối ưu triệt để bộ nhớ RAM.
    - Chuẩn hóa Schema Drift và gắn bộ 3 trường lineage (batch_id, source_file, ingested_at).
    - Nạp siêu tốc vào PostgreSQL qua cơ chế copy_expert (COPY FROM STDIN) của psycopg2.
    - Ghi nhận trạng thái 'STAGED' và số dòng thô vào ctrl.processed_files & ctrl.etl_run_log.
    """
    watermark_ctx = staged_payload.get("watermark_ctx", {})
    files_to_process = staged_payload.get("files_to_process", [])
    batch_id = staged_payload.get("batch_id")
    ingested_at = staged_payload.get("ingested_at")
    run_id = watermark_ctx.get("run_id")

    conn = get_pg_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    total_extracted_rows = 0
    files_summary = []

    try:
        # 1. Truncate staging table trước mỗi đợt nạp
        print(f"[export_to_staging] Truncating stage.stg_yellow_trips for batch {batch_id}...")
        cursor.execute("TRUNCATE TABLE stage.stg_yellow_trips;")

        # 2. Xử lý từng file Parquet
        copy_sql = (
            f"COPY stage.stg_yellow_trips ({','.join(STAGING_COLUMNS_ORDER)}) "
            f"FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')"
        )

        for file_info in files_to_process:
            file_path = file_info["file_path"]
            file_name = file_info["file_name"]
            file_mtime = file_info["file_mtime"]
            print(f"[export_to_staging] Loading file: {file_name}...")

            parquet_file = pq.ParquetFile(file_path)
            file_rows = 0

            # Đọc theo chunk để tránh tràn RAM
            for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
                df_chunk = batch.to_pandas()
                df_normalized = normalize_dataframe_batch(
                    df_chunk,
                    batch_id=batch_id,
                    source_file=file_name,
                    ingested_at=ingested_at,
                )

                # Xuất ra in-memory CSV buffer
                csv_buffer = io.StringIO()
                df_normalized.to_csv(csv_buffer, index=False, header=False, sep=",", na_rep="")
                csv_buffer.seek(0)

                cursor.copy_expert(copy_sql, csv_buffer)
                file_rows += len(df_normalized)

            total_extracted_rows += file_rows
            files_summary.append({
                "file_name": file_name,
                "file_mtime": file_mtime,
                "row_count": file_rows,
            })

            # Ghi nhận trạng thái tệp nguồn đã vào Staging
            cursor.execute(
                """
                INSERT INTO ctrl.processed_files (
                    file_name, file_mtime, batch_id, row_count, status, processed_at
                ) VALUES (
                    %s, %s, %s, %s, 'STAGED', CURRENT_TIMESTAMP
                )
                ON CONFLICT (file_name) DO UPDATE
                SET file_mtime = EXCLUDED.file_mtime,
                    batch_id = EXCLUDED.batch_id,
                    row_count = EXCLUDED.row_count,
                    status = 'STAGED',
                    processed_at = CURRENT_TIMESTAMP;
                """,
                (file_name, file_mtime, batch_id, file_rows),
            )
            print(f"[export_to_staging] Finished {file_name}: {file_rows:,} rows staged.")

        # 3. Cập nhật số dòng trích xuất vào ctrl.etl_run_log
        if run_id:
            cursor.execute(
                """
                UPDATE ctrl.etl_run_log
                SET files_processed = %s,
                    rows_extracted = %s,
                    records_extracted = %s
                WHERE run_id = %s;
                """,
                (len(files_to_process), total_extracted_rows, total_extracted_rows, run_id),
            )

        conn.commit()
        print(
            f"[export_to_staging] Batch {batch_id} complete: {len(files_to_process)} files, "
            f"{total_extracted_rows:,} total rows staged."
        )

    except Exception as exc:
        conn.rollback()
        err_msg = str(exc)
        print(f"[export_to_staging] Error during staging export: {err_msg}")
        try:
            with conn.cursor() as err_cur:
                if run_id:
                    err_cur.execute(
                        """
                        UPDATE ctrl.etl_run_log
                        SET status = 'FAILED', error_message = %s, finished_at = CURRENT_TIMESTAMP
                        WHERE run_id = %s;
                        """,
                        (err_msg, run_id),
                    )
                err_cur.execute(
                    """
                    UPDATE ctrl.etl_watermark
                    SET last_status = 'FAILED', updated_at = CURRENT_TIMESTAMP
                    WHERE pipeline_name = 'etl_taxi_trips';
                    """
                )
                conn.commit()
        except Exception:
            pass
        raise
    finally:
        cursor.close()
        conn.close()

    staged_payload["total_extracted_rows"] = total_extracted_rows
    staged_payload["files_summary"] = files_summary
    return staged_payload
