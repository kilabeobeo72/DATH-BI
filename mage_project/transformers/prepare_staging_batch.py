if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def prepare_staging_batch(extract_payload, *args, **kwargs):
    """
    Bước 3 (Thành viên 2 - Ý):
    - Đọc từng file Parquet, chuẩn hóa Schema Drift (Airport_fee -> airport_fee, ép kiểu chuẩn).
    - Gắn cột lineage: batch_id, source_file, ingested_at.
    """
    # TODO (Thành viên 2 - Ý): Chuẩn hóa cột & kiểu dữ liệu trước khi export vào stage.stg_yellow_trips
    return extract_payload
