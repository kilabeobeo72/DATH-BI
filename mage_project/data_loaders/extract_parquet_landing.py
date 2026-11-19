from datetime import datetime
import glob
import os

if 'data_loader' not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        def data_loader(fn):
            return fn

from mage_project.custom.db_utils import get_db_cursor


@data_loader
def extract_parquet_landing(watermark_ctx, *args, **kwargs):
    """
    Bước 2 (Thành viên 2 - Thạch Ý):
    - Quét thư mục DATA_LANDING_DIR bằng glob (hỗ trợ cả file ở root và file trong subfolder năm 2023, 2024).
    - Lọc đúng tệp Yellow Taxi: yellow_tripdata_*.parquet (loại bỏ green taxi hoặc file tạm).
    - Tra cứu danh sách các file đã xử lý thành công trong ctrl.processed_files.
    - Áp dụng cơ chế Ingestion Window:
        + Full Load: quét toàn bộ file chưa được nạp (hoặc toàn bộ file nếu full_refresh).
        + Incremental Load: chỉ tiếp nhận các file mới có file_mtime thuộc (LSET, CET] và chưa có trong ctrl.processed_files.
    - Sắp xếp thứ tự nạp theo trình tự thời gian (chronological).
    - Hỗ trợ biến batch_size để chia đợt nạp nếu cần tối ưu tài nguyên.
    """
    landing_dir = os.getenv("DATA_LANDING_DIR", "/home/src/data/landing")
    if not os.path.exists(landing_dir):
        # Fallback cho chạy local ngoài container
        local_candidate = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "landing"))
        if os.path.exists(local_candidate):
            landing_dir = local_candidate

    load_mode = watermark_ctx.get("load_mode", "incremental")
    lset_str = watermark_ctx.get("lset", "1900-01-01 00:00:00")
    cet_str = watermark_ctx.get("cet")

    lset_dt = datetime.strptime(lset_str, "%Y-%m-%d %H:%M:%S")
    cet_dt = datetime.strptime(cet_str, "%Y-%m-%d %H:%M:%S")

    # 1. Quét toàn bộ file parquet
    search_pattern = os.path.join(landing_dir, "**", "*.parquet")
    all_files = glob.glob(search_pattern, recursive=True)

    # 2. Lấy danh sách các file đã nạp từ database
    loaded_files = set()
    with get_db_cursor() as (conn, cursor):
        cursor.execute("SELECT file_name FROM ctrl.processed_files WHERE status = 'LOADED';")
        for row in cursor.fetchall():
            loaded_files.add(row[0])

    # 3. Lọc và chuẩn hóa danh sách file
    seen_file_names = set()
    eligible_files = []

    for file_path in all_files:
        file_name = os.path.basename(file_path)
        # Chỉ nhận đúng file Yellow Taxi
        if not (file_name.startswith("yellow_tripdata_") and file_name.endswith(".parquet")):
            continue

        # Tránh trùng lặp tên file nếu file tồn tại ở nhiều thư mục
        if file_name in seen_file_names:
            continue
        seen_file_names.add(file_name)

        mtime_epoch = os.path.getmtime(file_path)
        file_mtime = datetime.fromtimestamp(mtime_epoch)
        file_size = os.path.getsize(file_path)

        if load_mode == "full":
            # Trong Full Load, ưu tiên nạp các file chưa nạp (hoặc toàn bộ nếu chạy lại)
            if file_name not in loaded_files:
                eligible_files.append({
                    "file_path": file_path,
                    "file_name": file_name,
                    "file_mtime": file_mtime.strftime("%Y-%m-%d %H:%M:%S"),
                    "file_size": file_size,
                })
        else:
            # Incremental Load: LSET < file_mtime <= CET và chưa từng nạp thành công
            # Đồng thời nếu file chưa nạp và file_mtime <= CET cũng được nhận diện an toàn
            if file_name not in loaded_files and file_mtime <= cet_dt:
                if lset_str == "1900-01-01 00:00:00" or file_mtime > lset_dt or file_name not in loaded_files:
                    eligible_files.append({
                        "file_path": file_path,
                        "file_name": file_name,
                        "file_mtime": file_mtime.strftime("%Y-%m-%d %H:%M:%S"),
                        "file_size": file_size,
                    })

    # 4. Sắp xếp file theo trình tự thời gian
    eligible_files.sort(key=lambda x: x["file_name"])

    # 5. Phân batch (nếu có tham số batch_size truyền vào)
    batch_size_arg = kwargs.get("batch_size")
    if batch_size_arg:
        try:
            batch_size = int(batch_size_arg)
            if batch_size > 0:
                eligible_files = eligible_files[:batch_size]
        except (ValueError, TypeError):
            pass

    print(
        f"[extract_parquet_landing] Mode={load_mode}, Ingestion Window=({lset_str}, {cet_str}], "
        f"Found {len(eligible_files)} files to process: {[f['file_name'] for f in eligible_files]}"
    )

    return {
        "watermark_ctx": watermark_ctx,
        "files_to_process": eligible_files,
        "total_files_found": len(eligible_files),
    }
