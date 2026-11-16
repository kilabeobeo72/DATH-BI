from datetime import datetime
import uuid

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_watermark_and_cet(*args, **kwargs):
    """
    Bước 1 (BẮT BUỘC):
    - Lấy CET (Current Extraction Time) NGAY TẠI THỜI ĐIỂM BẮT ĐẦU pipeline.
    - Đọc LSET từ ctrl.etl_watermark (nếu load_mode == 'full', dùng '1900-01-01 00:00:00').
    - Truyền context (lset, cet, batch_id, load_mode) cho các block phía sau.
    """
    cet = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    batch_id = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    load_mode = kwargs.get("load_mode", "incremental")

    # TODO (Thành viên 2 - Ý): Đọc LSET thực tế từ ctrl.etl_watermark và ghi log STARTED vào ctrl.etl_run_log
    lset = "1900-01-01 00:00:00" if load_mode == "full" else "1900-01-01 00:00:00"

    return {
        "pipeline_name": "etl_taxi_trips",
        "load_mode": load_mode,
        "batch_id": batch_id,
        "lset": lset,
        "cet": cet,
    }
