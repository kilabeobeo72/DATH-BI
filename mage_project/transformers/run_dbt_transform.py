import os
import subprocess

from mage_project.custom.db_utils import get_db_cursor

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

DBT_MODELS = ['stg_yellow_trips_clean', 'quarantine_log', 'fact_taxitrip']


def _run_dbt(args, dbt_dir):
    cmd = ['dbt', *args, '--project-dir', dbt_dir, '--profiles-dir', dbt_dir]
    print('>>', ' '.join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"dbt {args[0]} thất bại (exit code {result.returncode})")


def _mark_failed(payload, err_msg):
    watermark_ctx = (payload or {}).get('watermark_ctx', {})
    run_id = watermark_ctx.get('run_id')
    pipeline_name = watermark_ctx.get('pipeline_name', 'etl_taxi_trips')
    try:
        with get_db_cursor(commit=True) as (conn, cursor):
            if run_id:
                cursor.execute(
                    """
                    UPDATE ctrl.etl_run_log
                    SET status = 'FAILED', error_message = %s, finished_at = CURRENT_TIMESTAMP
                    WHERE run_id = %s;
                    """,
                    (err_msg, run_id),
                )
            cursor.execute(
                """
                UPDATE ctrl.etl_watermark
                SET last_status = 'FAILED', updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_name = %s;
                """,
                (pipeline_name,),
            )
    except Exception:
        pass


@transformer
def run_dbt_transform(payload, *args, **kwargs):
    # Chạy dbt run rồi dbt test; lỗi thì ghi FAILED và raise để commit_watermark_log không chạy.
    # dbt_full_refresh (mặc định False): True thì truncate fact và quarantine rồi nạp lại,
    # chỉ bật ở batch đầu tiên của Full Load.
    dbt_dir = os.getenv('DBT_PROJECT_DIR', '/home/src/dbt_project')
    full_refresh = str(kwargs.get('dbt_full_refresh', False)).lower() in ('1', 'true', 'yes')

    run_args = ['run', '--select', *DBT_MODELS]
    if full_refresh:
        run_args.append('--full-refresh')

    try:
        _run_dbt(run_args, dbt_dir)
        _run_dbt(['test'], dbt_dir)
    except Exception as exc:
        _mark_failed(payload, str(exc))
        raise

    return payload
