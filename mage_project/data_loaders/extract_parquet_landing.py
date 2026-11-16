if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def extract_parquet_landing(watermark_ctx, *args, **kwargs):
    """
    Bước 2 (Thành viên 2 - Ý):
    - Quét thư mục DATA_LANDING_DIR bằng glob.
    - Lọc các file chưa xử lý (ctrl.processed_files) và có file_mtime thuộc (LSET, CET].
    - Trả về danh sách file cần nạp (xử lý theo từng batch file để tránh tràn RAM).
    """
    # TODO (Thành viên 2 - Ý): Triển khai quét file & lọc theo (lset, cet]
    return {"watermark_ctx": watermark_ctx, "files_to_process": []}
