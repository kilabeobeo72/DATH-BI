from datetime import datetime
import pandas as pd

if 'transformer' not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        def transformer(fn):
            return fn


# Bản đồ ánh xạ cột giải quyết triệt để Schema Drift giữa 2023 và 2024
COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "vendor_id": "vendor_id",
    "RatecodeID": "ratecode_id",
    "ratecode_id": "ratecode_id",
    "PULocationID": "pu_location_id",
    "pu_location_id": "pu_location_id",
    "DOLocationID": "do_location_id",
    "do_location_id": "do_location_id",
    "Airport_fee": "airport_fee",
    "airport_fee": "airport_fee",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "tpep_pickup_datetime": "tpep_pickup_datetime",
    "tpep_dropoff_datetime": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
}

STAGING_COLUMNS_ORDER = [
    "batch_id",
    "source_file",
    "ingested_at",
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


def normalize_dataframe_batch(df: pd.DataFrame, batch_id: str, source_file: str, ingested_at: str) -> pd.DataFrame:
    """
    Chuẩn hóa một batch dữ liệu từ file Parquet (2023 hoặc 2024):
    1. Đổi tên cột đồng bộ snake_case (VendorID -> vendor_id, Airport_fee -> airport_fee...).
    2. Xử lý kiểu dữ liệu Schema Drift:
       - Ép kiểu INT64/DOUBLE sang đúng cấu trúc bảng stage.stg_yellow_trips.
       - passenger_count & ratecode_id: float sang nullable Int64 (giữ nguyên NaN/None cho staging).
       - Các trường số tiền: ép sang float64.
       - store_and_fwd_flag: strip chuỗi, gán None cho giá trị rỗng/nan.
    3. Gắn bộ 3 cột Lineage: batch_id, source_file, ingested_at.
    4. Sắp xếp đúng 22 cột của stage.stg_yellow_trips.
    """
    # 1. Đổi tên cột
    df = df.rename(columns=lambda col: COLUMN_MAPPING.get(col, col.lower()))

    # 2. Xử lý các cột số nguyên (nullable)
    int_cols = ["vendor_id", "ratecode_id", "pu_location_id", "do_location_id", "payment_type", "passenger_count"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")

    # 3. Xử lý các cột số thực (tiền tệ & khoảng cách)
    float_cols = [
        "trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "airport_fee",
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = 0.0

    # 4. Xử lý datetime
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    # 5. Xử lý store_and_fwd_flag
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = (
            df["store_and_fwd_flag"]
            .astype(str)
            .str.strip()
            .replace({"nan": None, "None": None, "<NA>": None, "": None})
        )
    else:
        df["store_and_fwd_flag"] = None

    # 6. Gắn cột lineage
    df["batch_id"] = batch_id
    df["source_file"] = source_file
    df["ingested_at"] = ingested_at

    return df[STAGING_COLUMNS_ORDER]


@transformer
def prepare_staging_batch(extract_payload, *args, **kwargs):
    """
    Bước 3 (Thành viên 2 - Thạch Ý):
    - Tiếp nhận danh sách tệp từ extract_parquet_landing.
    - Gắn timestamp ingested_at cho toàn bộ đợt nạp.
    - Chuẩn bị metadata và xác nhận cấu trúc trước khi đẩy sang tầng export.
    """
    watermark_ctx = extract_payload.get("watermark_ctx", {})
    files_to_process = extract_payload.get("files_to_process", [])
    batch_id = watermark_ctx.get("batch_id")
    ingested_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(
        f"[prepare_staging_batch] Ready to stage {len(files_to_process)} files for batch={batch_id} "
        f"at {ingested_at}"
    )

    return {
        "watermark_ctx": watermark_ctx,
        "files_to_process": files_to_process,
        "ingested_at": ingested_at,
        "batch_id": batch_id,
    }
