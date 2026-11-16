# Đồ án Thực hành CSC12107 — Kho Dữ liệu NYC Yellow Taxi (2023–2024)

## 1. Kiến trúc & Công nghệ
- **Kiến trúc 3 tầng (đã lược bỏ NDS):** `Source (Parquet/CSV)` → `Staging (stage)` → `DDS (dds)` + tầng điều khiển `ctrl` (và dự phòng `olap` cho Giai đoạn 3).
- **Công cụ:** Docker Compose, PostgreSQL 16, Mage.ai (`http://localhost:6789`), dbt-postgres.

## 2. Quy ước đã chốt
- **Schema chuẩn (chữ thường):** `ctrl`, `stage`, `dds`, `olap`.
- **`passenger_count = 0` hoặc `NULL`:** Chuẩn hóa về `0` và map vào `PassengerGroupKey = 4` ("Chưa xác định"). Chỉ loại bản ghi `< 0` hoặc `> 9`.
- **Chuyến xuyên đêm 31/12/2024 → 01/01/2025:** `Dim_Date` có **732 dòng** (`2023-01-01` đến `2025-01-01`). `PickupDateKey` lọc trong `[20230101, 20241231]`, `DropoffDateKey` cho phép đến `20250101`.
- **`TotalAmount`:** Cho phép `>= 0.00` với `payment_type IN (3, 4)` (`No Charge`, `Dispute`); các trường hợp còn lại bắt buộc `> 0.00`.
- **Cơ chế `CET` & `LSET`:** Lấy `CET` ngay tại thời điểm **bắt đầu** pipeline; lọc theo cửa sổ `(LSET, CET]` (kết hợp `ctrl.processed_files`); chỉ cập nhật `LSET = CET` ở block cuối cùng sau khi toàn bộ pipeline thành công.

## 3. Khởi chạy nhanh
1. Sao chép `.env.example` thành `.env` và điền mật khẩu.
2. Đặt `taxi_zone_lookup.csv` vào `data/lookup/` và các file `.parquet` vào `data/landing/`.
3. Chạy `docker compose up -d --build`.
4. Truy cập Mage UI tại `http://localhost:6789`.
