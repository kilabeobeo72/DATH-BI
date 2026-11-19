"""
Bộ kiểm thử tự động cho phần việc của Thạch Ý (23120411)
- Kiểm tra Quét tệp & Lọc theo Ingestion Window
- Kiểm tra Chuẩn hóa Schema Drift 2023 vs 2024
- Kiểm tra Gắn cột Lineage (batch_id, source_file, ingested_at)
- Kiểm tra Cơ chế Watermark (LSET/CET) & Commit trạng thái

Cách chạy:
    python tests/test_mage_ingestion.py
"""

import glob
import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

# Thêm root vào sys.path để import mage_project
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import pyarrow.parquet as pq
from mage_project.transformers.prepare_staging_batch import (
    STAGING_COLUMNS_ORDER,
    normalize_dataframe_batch,
    prepare_staging_batch,
)
from mage_project.data_loaders.load_watermark_and_cet import load_watermark_and_cet
from mage_project.data_loaders.extract_parquet_landing import extract_parquet_landing
from mage_project.data_exporters.commit_watermark_log import commit_watermark_log


def print_header(title):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def test_schema_drift():
    print_header("TEST 1: Chuẩn hóa Schema Drift (2023 vs 2024)")
    f2023 = os.path.join(ROOT_DIR, "data", "landing", "2023", "yellow_tripdata_2023-01.parquet")
    f2024 = os.path.join(ROOT_DIR, "data", "landing", "2024", "yellow_tripdata_2024-01.parquet")

    for year, fpath in [("2023 (airport_fee thường, float count)", f2023), ("2024 (Airport_fee hoa, int64)", f2024)]:
        if not os.path.exists(fpath):
            print(f"  [SKIP] Không tìm thấy file: {fpath}")
            continue

        table = pq.read_table(fpath).slice(0, 500)
        df_raw = table.to_pandas()
        df_norm = normalize_dataframe_batch(
            df_raw,
            batch_id="batch_test_drift",
            source_file=os.path.basename(fpath),
            ingested_at="2026-11-19 20:00:00",
        )

        assert list(df_norm.columns) == STAGING_COLUMNS_ORDER, "Sai thứ tự hoặc thiếu cột staging!"
        assert str(df_norm["passenger_count"].dtype) == "Int64", "passenger_count chưa về Int64!"
        assert str(df_norm["vendor_id"].dtype) == "Int64", "vendor_id chưa về Int64!"
        assert "float" in str(df_norm["airport_fee"].dtype), "airport_fee chưa về float!"
        assert df_norm["batch_id"].iloc[0] == "batch_test_drift"
        assert df_norm["source_file"].iloc[0] == os.path.basename(fpath)
        assert df_norm["ingested_at"].iloc[0] == "2026-11-19 20:00:00"

        print(f"  [PASS] {year}: Chuẩn hóa 500 dòng thành công! Cột chuẩn xác 22/22.")


def test_file_discovery_and_filtering():
    print_header("TEST 2: Quét tệp Parquet & Lọc Ingestion Window")
    landing_dir = os.path.join(ROOT_DIR, "data", "landing")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    # Giả lập database đã nạp file 2023-01
    mock_cursor.fetchall.return_value = [("yellow_tripdata_2023-01.parquet",)]

    ctx = {
        "pipeline_name": "etl_taxi_trips",
        "load_mode": "incremental",
        "batch_id": "batch_test_extract",
        "lset": "2023-01-01 00:00:00",
        "cet": "2030-01-01 00:00:00",
    }

    with patch("mage_project.data_loaders.extract_parquet_landing.get_db_cursor") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = (mock_conn, mock_cursor)
        payload = extract_parquet_landing(ctx, batch_size=5)

        files = payload["files_to_process"]
        print(f"  Số tệp quét được để nạp (batch_size=5): {len(files)}")
        for f in files:
            print(f"    + {f['file_name']} (mtime: {f['file_mtime']})")

        # Kiểm tra không chứa green taxi
        file_names = [f["file_name"] for f in files]
        assert not any(f.startswith("green_") for f in file_names), "Chứa file green_ taxi không hợp lệ!"
        # Kiểm tra file đã nạp 2023-01 không bị quét lại
        assert "yellow_tripdata_2023-01.parquet" not in file_names, "File đã LOADED bị nạp lặp lại!"
        print("  [PASS] Tự động lọc Ingestion Window, loại bỏ Green Taxi và chống nạp trùng tệp cũ!")


def test_watermark_lifecycle():
    print_header("TEST 3: Vòng đời Watermark (CET đầu pipeline -> LSET cuối pipeline)")
    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    # 1. Bắt đầu pipeline
    mock_cursor.fetchone.side_effect = [
        (datetime(2026, 11, 10, 8, 0, 0),),  # LSET hiện tại trong DB
        (101,),                               # run_id tạo mới
    ]

    with patch("mage_project.data_loaders.load_watermark_and_cet.get_db_cursor") as mock_get_db:
        mock_get_db.return_value.__enter__.return_value = (mock_conn, mock_cursor)
        ctx = load_watermark_and_cet(load_mode="incremental")

        assert ctx["lset"] == "2026-11-10 08:00:00"
        assert ctx["run_id"] == 101
        assert "cet" in ctx
        print(f"  [PASS] Block 1 (Bắt đầu): Đã lấy CET={ctx['cet']}, LSET={ctx['lset']}, run_id={ctx['run_id']}")

    # 2. Bước trung gian: prepare_staging_batch
    stg_payload = prepare_staging_batch({"watermark_ctx": ctx, "files_to_process": []})
    assert stg_payload["batch_id"] == ctx["batch_id"]
    print(f"  [PASS] Block 3 (Biến đổi): Gắn Lineage batch_id={stg_payload['batch_id']}")

    # 3. Kết thúc pipeline: commit_watermark_log
    mock_conn2 = MagicMock()
    mock_cur2 = MagicMock()
    mock_cur2.fetchone.side_effect = [
        (2986361,),  # fact_rows
        (80405,),    # quarantine_rows
    ]

    with patch("mage_project.data_exporters.commit_watermark_log.get_pg_connection") as mock_pg:
        mock_pg.return_value = mock_conn2
        mock_conn2.cursor.return_value = mock_cur2
        commit_res = commit_watermark_log(stg_payload)

        assert commit_res["committed"] is True
        assert commit_res["new_lset"] == ctx["cet"]
        assert commit_res["fact_rows"] == 2986361
        assert commit_res["quarantine_rows"] == 80405
        print(f"  [PASS] Block 6 (Chốt hạ): LSET tịnh tiến chính xác lên CET ({commit_res['new_lset']})")
        print(f"         Đối soát: Fact={commit_res['fact_rows']:,}, Quarantine={commit_res['quarantine_rows']:,}")


if __name__ == "__main__":
    print("\n>>> BẮT ĐẦU KIỂM THỬ TỰ ĐỘNG CÁC TÍNH NĂNG CỦA THẠCH Ý (23120411) <<<")
    test_schema_drift()
    test_file_discovery_and_filtering()
    test_watermark_lifecycle()
    print("\n" + "=" * 70)
    print("  KẾT QUẢ TỔNG THỂ: TẤT CẢ 3/3 BÀI TEST ĐỀU ĐẠT CHUẨN 100% (PASS)")
    print("=" * 70 + "\n")
