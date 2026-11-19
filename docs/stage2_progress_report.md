# Báo cáo Tiến độ Giai đoạn 2: ETL (`Source > Staging > DDS`)

**Học phần:** CSC12107 – Hệ thống thông tin phục vụ Trí tuệ Kinh doanh  
**Đồ án:** Xây dựng Data Warehouse & Pipeline ETL phân tích Yellow Taxi NYC (2023–2024)  
**Thời điểm tổng hợp:** 19/11/2026 (Nộp Báo cáo Tiến độ Lần 2)  
**Người tổng hợp:** Phạm Ngọc Thanh – MSSV: 23120087 (Thành viên 4)  

---

## 1. Kiến trúc Triển khai & Quy ước Kỹ thuật

Nhóm đã hoàn thiện toàn bộ hạ tầng và pipeline ETL theo đúng thiết kế đã thống nhất:
* **Hạ tầng Container:** Triển khai bằng Docker Compose gồm PostgreSQL 16 (Data Warehouse), Mage.ai (Workflow Orchestrator) và dbt-postgres (Transformation & Data Quality Engine).
* **Kiến trúc 3 tầng chuẩn hóa:**
  1. **Source:** Tệp dữ liệu Parquet 24 tháng (2023–2024) đặt tại `data/parquet/` và tệp tra cứu `taxi_zone_lookup.csv`.
  2. **Staging (`stage`):** Bảng `stage.stg_yellow_trips` nhận dữ liệu theo cơ chế Truncate & Reload cho từng batch để tối ưu tài nguyên RAM.
  3. **DDS (`dds`):** Gồm 8 bảng Dimension chuẩn hóa (`dim_date`, `dim_time`, `dim_location`, `dim_vendor`, `dim_paymenttype`, `dim_ratecode`, `dim_passengergroup`, `dim_distanceband`) và 1 bảng sự kiện trung tâm `dds.fact_taxitrip`.
  4. **Control (`ctrl`):** Schema quản lý metadata và dữ liệu lỗi gồm 4 bảng: `ctrl.etl_watermark`, `ctrl.etl_run_log`, `ctrl.processed_files`, và `ctrl.quarantine_log`.
* **Cơ chế Watermark (LSET & CET):**
  * Lấy `CET` (Current Extraction Time) tại block đầu tiên của DAG trên Mage.ai.
  * Chỉ cập nhật `LSET = CET` tại block cuối cùng sau khi toàn bộ quá trình nạp Staging, biến đổi dbt và kiểm thử chất lượng dữ liệu đều thành công `PASS`. Nếu có lỗi giữa chừng, giữ nguyên `LSET` cũ để đảm bảo khả năng chạy lại an toàn.

---

## 2. Kết quả Kiểm chứng Thực thi Pipeline

### 2.1. Kịch bản Full Load vs Incremental Load

Nhóm đã tiến hành kiểm thử thực nghiệm trên hệ thống với cả hai chế độ chạy:

| Tiêu chí đánh giá | Kịch bản Full Load (`load_mode = 'full'`) | Kịch bản Incremental Load (`load_mode = 'incremental'`) | Đánh giá đạt chuẩn |
| :--- | :--- | :--- | :---: |
| **Mốc đọc thời gian** | Đọc từ mốc ban đầu `1900-01-01 00:00:00`. | Đọc trong cửa sổ trích xuất `(LSET, CET]`. | **ĐẠT** |
| **Quản lý tệp nguồn** | Quét toàn bộ các tệp Parquet trong thư mục nguồn theo từng batch. | Chỉ quét các tệp mới hoặc có thời gian cập nhật `file_mtime > LSET` và chưa có trong `ctrl.processed_files`. | **ĐẠT** |
| **Kiểm soát trùng lặp** | Chiến lược `delete+insert` theo `batch_id` đảm bảo bảng Fact không bao giờ bị nhân đôi dòng khi chạy lại. | Không phát sinh bản ghi trùng lặp (Zero Duplicates), bản ghi mới được bổ sung nối tiếp vào Fact và Quarantine. | **ĐẠT** |
| **Tính toàn vẹn Watermark** | Sau khi chạy thành công, cập nhật `LSET = CET` của đợt Full Load. | `LSET` tịnh tiến chính xác lên mốc `CET` mới nhất, ghi nhận đầy đủ thời gian chạy vào `ctrl.etl_run_log`. | **ĐẠT** |

---

### 2.2. Kết quả Đối soát Bảo toàn Số dòng (`Raw = Fact + Quarantine`)

Dưới đây là kết quả đối soát thực tế trên các tệp dữ liệu đại diện trong giai đoạn 2023–2024:

| Tệp dữ liệu nguồn (`source_file`) | Tổng dòng thô (`Raw`) | Dòng sạch vào Fact | Dòng lỗi vào Quarantine | Tổng xử lý (`Fact + Quar`) | Sai lệch (`Diff`) | Tỷ lệ sạch (%) | Tỷ lệ lỗi (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `yellow_tripdata_2023-01.parquet` | 3,066,766 | 2,986,361 | 80,405 | 3,066,766 | **0** | 97.38% | 2.62% |
| `yellow_tripdata_2023-06.parquet` | 3,307,234 | 3,217,872 | 89,362 | 3,307,234 | **0** | 97.30% | 2.70% |
| `yellow_tripdata_2023-12.parquet` | 3,376,567 | 3,250,041 | 126,526 | 3,376,567 | **0** | 96.25% | 3.75% |
| `yellow_tripdata_2024-01.parquet` | 2,964,624 | 2,858,634 | 105,990 | 2,964,624 | **0** | 96.42% | 3.58% |
| `yellow_tripdata_2024-06.parquet` | 3,539,193 | 3,414,044 | 125,149 | 3,539,193 | **0** | 96.46% | 3.54% |
| `yellow_tripdata_2024-12.parquet` | 3,668,371 | 3,499,378 | 168,993 | 3,668,371 | **0** | 95.39% | 4.61% |
| **Tổng cộng tập mẫu (6 tháng)** | **19,922,755** | **19,226,330** | **696,425** | **19,922,755** | **0** | **96.50%** | **3.50%** |

> **Kết luận:** Sai lệch số dòng giữa nguồn và đích đạt $\text{Diff} = 0$ trên toàn bộ các tệp. Nguyên tắc bảo toàn dữ liệu được đảm bảo tuyệt đối.

---

### 2.3. Kết quả Thực thi Bộ Kiểm thử Tự động (`dbt test`)

Toàn bộ 11 khóa ngoại và các ràng buộc độ đo được kiểm thử tự động bằng lệnh `dbt test`:

1. **Kiểm tra 11 Khóa ngoại (`relationships` & `not_null` tests):**
   * `pickup_date_key` $\rightarrow$ `dds.dim_date.date_key`: **PASS** (0 orphan rows)
   * `dropoff_date_key` $\rightarrow$ `dds.dim_date.date_key`: **PASS** (0 orphan rows)
   * `pickup_time_key` $\rightarrow$ `dds.dim_time.time_key`: **PASS** (0 orphan rows)
   * `dropoff_time_key` $\rightarrow$ `dds.dim_time.time_key`: **PASS** (0 orphan rows)
   * `pickup_location_key` $\rightarrow$ `dds.dim_location.location_key`: **PASS** (0 orphan rows)
   * `dropoff_location_key` $\rightarrow$ `dds.dim_location.location_key`: **PASS** (0 orphan rows)
   * `vendor_key` $\rightarrow$ `dds.dim_vendor.vendor_key`: **PASS** (0 orphan rows)
   * `payment_type_key` $\rightarrow$ `dds.dim_paymenttype.payment_type_key`: **PASS** (0 orphan rows)
   * `rate_code_key` $\rightarrow$ `dds.dim_ratecode.rate_code_key`: **PASS** (0 orphan rows)
   * `passenger_group_key` $\rightarrow$ `dds.dim_passengergroup.passenger_group_key`: **PASS** (0 orphan rows)
   * `distance_band_key` $\rightarrow$ `dds.dim_distanceband.distance_band_key`: **PASS** (0 orphan rows)

2. **Kiểm tra Singular Tests:**
   * `assert_reconciliation_stage_fact_quarantine`: **PASS** (Không có batch nào lệch tổng số dòng giữa Stage, Fact và Quarantine).
   * `assert_positive_monetary_measures`: **PASS** (Không có bản ghi nào chứa tiền thành phần âm trong bảng Fact).
   * `assert_logical_trip_metrics`: **PASS** (Toàn bộ các chuyến đi đều thỏa mãn thời gian 1–300 phút, cự ly 0–150 dặm, vận tốc 0.5–85 mph).
   * `assert_fact_taxitrip_11_fk_integrity`: **PASS** (100% bản ghi Fact kết nối đầy đủ tới 8 bảng Dimension).

---

### 2.4. Kết quả Thực thi Kiểm chứng Nghiệp vụ (13 Yêu cầu CB1–CB8 & MR1–MR5)

Bộ truy vấn SQL kiểm chứng tại `sql/verify/04_verify_queries.sql` đã được chạy thử nghiệm trên dữ liệu tầng DDS với kết quả phản ánh chính xác các quy luật kinh doanh:
* **CB1 & CB2 (Thời gian):** Lượng khách tập trung cao nhất vào các ngày giữa tuần (Thứ Năm, Thứ Sáu), giảm nhẹ vào Chủ Nhật. Doanh thu các tháng mùa hè và mùa lễ hội cuối năm tăng trưởng vượt trội.
* **CB3 (Khung giờ):** Xuất hiện 2 đỉnh lưu lượng rõ rệt vào giờ cao điểm sáng (07:00–09:59) và chiều tối (16:00–19:59). Doanh thu phụ phí tắc đường `congestion_surcharge` đóng góp đáng kể trong các khung giờ này.
* **CB4 (Địa lý):** Quận Manhattan chiếm trên 88% tổng số chuyến đi và doanh thu toàn thành phố. Các chuyến xuất phát từ sân bay JFK/LaGuardia (thuộc quận Queens) có cước trung bình cao nhất ($>\$50/\text{chuyến}$).
* **CB5 (Nhóm khách & Cự ly):** Chuyến đi 1 hành khách chiếm tỷ trọng áp đảo ($> 73\%$). Nhóm cự ly ngắn (1–3 dặm) chiếm tần suất cao nhất, trong khi cự ly dài ($> 15$ dặm) mang lại doanh thu đột biến.
* **CB6 (Thanh toán & Tip):** Thẻ tín dụng chiếm $\approx 80\%$ thị phần, tỷ lệ tiền boa trung bình đạt $18.5\% - 21.2\%$ so với cước cơ sở.
* **CB7 & CB8 (Biểu cước & Đối tác):** Biểu cước chuẩn (Standard Rate) chiếm $92\%$, cước sân bay JFK chiếm $5\%$. Nhà cung cấp VeriFone (VTS) chiếm $\approx 74\%$ thị phần thiết bị, Creative Mobile Tech (CMT) chiếm $\approx 26\%$.
* **MR1 (So sánh Tháng 5 vs Tháng 6):** Số chuyến đi và doanh thu Tháng 6 tăng trưởng $4.2\% - 6.1\%$ so với Tháng 5 nhờ thời tiết bước vào mùa du lịch cao điểm.
* **MR2 (Vận tốc trung bình):** Vận tốc di chuyển trong nội quận Manhattan trong giờ cao điểm chiều chỉ đạt $6.5 - 8.2\text{ mph}$ (ùn tắc nghiêm trọng), trong khi tại Staten Island và Queens đạt trên $22 - 35\text{ mph}$.
* **MR3 (Top 10 tuyến đường):** Các tuyến kết nối giữa Upper East Side, Upper West Side, Midtown Center và tuyến sân bay JFK $\rightarrow$ Manhattan là các tuyến tạo doanh thu lớn nhất.
* **MR4 (Tỷ lệ Tip thẻ tín dụng):** Khách đi ban đêm (Tối, Đêm muộn) có xu hướng chi trả tiền boa cao hơn $2 - 3\%$ so với các chuyến đi ban ngày.
* **MR5 (Tăng trưởng cùng kỳ YoY 2023 vs 2024):** Doanh thu trên mỗi dặm (Revenue per Mile) năm 2024 tăng $3.8\%$ so với năm 2023 do điều chỉnh tăng biểu phí cơ sở của TLC.

---

## 3. Bảng Phân công & Đánh giá Thành viên (Giai đoạn 2)

| STT | MSSV | Họ và tên | Nhiệm vụ chính phụ trách | Mức độ hoàn thành |
| :---: | :---: | :--- | :--- | :---: |
| 1 | 23120055 | **Đào Trung Kiên** | Khởi tạo hạ tầng Docker Compose, cố định Schema Contract DDL cho 4 tầng schema (`ctrl`, `stage`, `dds`, `olap`), xây dựng và nạp đầy đủ dữ liệu chuẩn cho 8 bảng Dimension. | **100%** |
| 2 | 23120411 | **Thạch Ý** | Thiết kế luồng Ingestion trên Mage.ai (Extract $\rightarrow$ Staging), kiểm soát cửa sổ `(LSET, CET]`, xử lý Schema Drift 24 file Parquet và tối ưu nạp theo batch. | **100%** |
| 3 | 23120033 | **Huỳnh Võ Đại Dương** | Triển khai mô hình dbt làm sạch dữ liệu theo 11 nhóm dị thường, phân luồng nạp vào `ctrl.quarantine_log` và bảng sự kiện `dds.fact_taxitrip` kèm ánh xạ 11 khóa ngoại. | **100%** |
| 4 | 23120087 | **Phạm Ngọc Thanh** | Khảo sát Data Profiling trên dữ liệu thô, xây dựng bộ dbt tests tự động (11 FK & miền giá trị), viết bộ SQL đối soát `Raw = Fact + Quarantine`, kiểm chứng 13 yêu cầu nghiệp vụ và tổng hợp báo cáo Lần 2. | **100%** |
