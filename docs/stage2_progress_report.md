# Báo cáo Tiến độ Giai đoạn 2: ETL (`Source > Staging > DDS`)

*(Tổng hợp nộp tiến độ Lần 2 — 19/11/2026)*

## 1. Kiến trúc triển khai & Quy ước
- Docker Compose (PostgreSQL 16 + Mage.ai + dbt-postgres)
- Cơ chế Full Load & Incremental Load với `CET` lấy đầu pipeline và `LSET` cập nhật cuối pipeline.

## 2. Kết quả chạy kiểm thử
- **Full Load**: ...
- **Incremental Load**: ...
- **Đối soát số dòng (`Raw = Fact + Quarantine`)**: ...
- **Kết quả `dbt test`**: ...

## 3. Phân công & Đánh giá thành viên
| MSSV | Họ và tên | Mục tiêu phụ trách | Mức độ hoàn thành |
| :--- | :--- | :--- | :--- |
| 23120055 | Đào Trung Kiên | Hạ tầng Docker/DB & 8 bảng Dim | ... |
| 23120411 | Thạch Ý | Mage Ingestion (`Source -> Staging`) & Watermark | ... |
| 23120033 | Huỳnh Võ Đại Dương | dbt Transformation (`Staging -> DDS`), Quarantine & Fact | ... |
| 23120087 | Phạm Ngọc Thanh | Data Profiling, Bộ kiểm thử & Tài liệu Lần 2 | ... |
