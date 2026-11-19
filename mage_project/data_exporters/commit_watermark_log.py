if 'data_exporter' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        def data_exporter(fn):
            return fn

from mage_project.custom.db_utils import get_pg_connection


@data_exporter
def commit_watermark_log(payload, *args, **kwargs):
    """
    Bước cuối cùng của DAG (Thành viên 2 - Thạch Ý):
    - CHỈ được thực thi sau khi export_to_staging + dbt run + dbt test đều thành công 100%.
    - Nếu có bất kỳ lỗi nào ở các bước trước, block này không chạy và LSET giữ nguyên mốc cũ.
    - Cập nhật LSET = CET tại ctrl.etl_watermark (đồng bộ cả 2 trường last_successful_extraction_time & lset).
    - Cập nhật trạng thái 'LOADED' cho toàn bộ các file của batch_id trong ctrl.processed_files.
    - Đối soát số dòng nạp Fact và Quarantine rồi hoàn tất nhật ký ctrl.etl_run_log với trạng thái 'SUCCESS'.
    """
    watermark_ctx = payload.get("watermark_ctx", {})
    batch_id = payload.get("batch_id") or watermark_ctx.get("batch_id")
    cet = watermark_ctx.get("cet")
    run_id = watermark_ctx.get("run_id")
    load_mode = watermark_ctx.get("load_mode", "incremental")

    conn = get_pg_connection()
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        # 1. Đếm số dòng đã vào Fact và Quarantine theo batch_id
        cursor.execute("SELECT COUNT(*) FROM dds.fact_taxitrip WHERE batch_id = %s;", (batch_id,))
        fact_count = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM ctrl.quarantine_log WHERE batch_id = %s;", (batch_id,))
        quarantine_count = cursor.fetchone()[0]

        # 2. Cập nhật trạng thái các file đã nạp thành công vào DDS
        cursor.execute(
            """
            UPDATE ctrl.processed_files
            SET status = 'LOADED', processed_at = CURRENT_TIMESTAMP
            WHERE batch_id = %s;
            """,
            (batch_id,),
        )

        # 3. Cập nhật nhật ký hoàn thành vào ctrl.etl_run_log
        if run_id:
            cursor.execute(
                """
                UPDATE ctrl.etl_run_log
                SET rows_loaded_fact = %s,
                    records_fact_inserted = %s,
                    rows_quarantined = %s,
                    records_quarantined = %s,
                    status = 'SUCCESS',
                    finished_at = CURRENT_TIMESTAMP
                WHERE run_id = %s;
                """,
                (fact_count, fact_count, quarantine_count, quarantine_count, run_id),
            )

        # 4. Cập nhật Watermark LSET = CET
        cursor.execute(
            """
            UPDATE ctrl.etl_watermark
            SET last_successful_extraction_time = %s,
                lset = %s,
                cet = %s,
                last_batch_id = %s,
                last_status = 'SUCCESS',
                last_run_mode = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE pipeline_name = 'etl_taxi_trips';
            """,
            (cet, cet, cet, batch_id, load_mode),
        )

        conn.commit()

        print(
            f"[commit_watermark_log] SUCCESS: Batch {batch_id} committed. "
            f"LSET updated to {cet}. Fact rows: {fact_count:,}, Quarantine rows: {quarantine_count:,}"
        )

        return {
            "committed": True,
            "pipeline_name": "etl_taxi_trips",
            "batch_id": batch_id,
            "new_lset": cet,
            "fact_rows": fact_count,
            "quarantine_rows": quarantine_count,
            "status": "SUCCESS",
        }

    except Exception as exc:
        conn.rollback()
        err_msg = str(exc)
        print(f"[commit_watermark_log] Error committing watermark: {err_msg}")
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
