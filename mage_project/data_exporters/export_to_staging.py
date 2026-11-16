if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_to_staging(staged_payload, *args, **kwargs):
    """
    Bước 4 (Thành viên 2 - Ý):
    - Truncate stage.stg_yellow_trips (chiến lược Truncate & Reload theo batch).
    - Nạp dữ liệu đã chuẩn hóa schema vào stage.stg_yellow_trips (dùng psycopg2 COPY để tối ưu tốc độ).
    """
    # TODO (Thành viên 2 - Ý): Nạp dữ liệu vào PostgreSQL schema stage
    return staged_payload
