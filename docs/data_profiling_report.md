# Báo cáo Data Profiling & Chiến thuật Xử lý Dữ liệu (Giai đoạn 2)

**Học phần:** CSC12107 – Hệ thống thông tin phục vụ Trí tuệ Kinh doanh  
**Đồ án:** Xây dựng Data Warehouse & Pipeline ETL phân tích Yellow Taxi NYC (2023–2024)  
**Người thực hiện:** Phạm Ngọc Thanh – MSSV: 23120087  
**Vai trò:** Thành viên 4 — Data Profiling, QA / Testing & Tổng hợp báo cáo  

---

## 1. Tổng quan Khảo sát & Profiling Dữ liệu Nguồn

Dữ liệu nguồn được cung cấp dưới dạng các tệp Parquet theo từng tháng trong giai đoạn 24 tháng liên tục từ tháng 01/2023 đến tháng 12/2024. Để phục vụ xây dựng chiến thuật làm sạch dữ liệu và thiết lập bộ kiểm soát chất lượng (Data Quality Gate) tại tầng Staging và DDS, nhóm đã thực hiện Data Profiling toàn diện trên tập mẫu gần 20 triệu bản ghi đại diện (gồm các tháng 01/2023, 06/2023, 12/2023, 01/2024, 06/2024, 12/2024).

### Bảng thống kê tỷ lệ dị thường dữ liệu (Data Anomalies Breakdown)

| STT | Nhóm dị thường / Lỗi dữ liệu | Số lượng vi phạm mẫu | Tỷ lệ (%) | Chiến thuật xử lý | Lý do nghiệp vụ & Kỹ thuật |
| :---: | :--- | :---: | :---: | :--- | :--- |
| **1** | **Thời lượng bất thường (`DURATION_INVALID`)** | 250,592 | 1.26% | **Quarantine** (`ctrl.quarantine_log`) | Chuyến đi có thời gian trả trước thời gian đón (`dropoff <= pickup`), hoặc thời lượng `< 1 phút` (chuyến hủy tại chỗ nhưng vẫn bấm đồng hồ), hoặc `> 300 phút` (5 giờ, lỗi quên tắt đồng hồ đo của tài xế). |
| **2** | **Khoản tiền thành phần âm (`NEGATIVE_AMOUNT`)** | 239,393 | 1.20% | **Quarantine** (`ctrl.quarantine_log`) | Các khoản phí (`fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `surcharges`) có giá trị `< 0`. Đây là các giao dịch điều chỉnh âm (void/refund) từ hệ thống POS, không đại diện cho hành trình thực tế. |
| **3** | **Quãng đường bất thường (`DISTANCE_INVALID`)** | 193,667 | 0.97% | **Quarantine** (`ctrl.quarantine_log`) | Quãng đường `trip_distance <= 0` hoặc `> 150 dặm` (vượt quá bán kính hoạt động hợp lệ của taxi New York). Các cuốc xe cự ly bằng 0 nhưng tính tiền là lỗi GPS hoặc hủy chuyến. |
| **4** | **Vận tốc phi thực tế (`SPEED_INVALID`)** | 12,461 | 0.06% | **Quarantine** (`ctrl.quarantine_log`) | Vận tốc tức thời tính toán `implied_speed_mph` ngoài khoảng `[0.5, 85.0]` mph. Vận tốc > 85 mph trong nội đô NYC vi phạm vật lý, còn < 0.5 mph kéo dài phản ánh cuốc xe kẹt xe bất động hoặc lỗi đồng hồ. |
| **5** | **Tổng tiền không hợp lệ (`TOTAL_AMOUNT_INVALID`)** | 246 | < 0.01% | **Quarantine** (`ctrl.quarantine_log`) | `total_amount <= 0` với các chuyến trả bằng thẻ tín dụng hoặc tiền mặt (`payment_type NOT IN (3, 4)`). Ngoại lệ cho phép total = 0 đối với loại 3 (No Charge) và 4 (Dispute). |
| **6** | **Lệch mốc thời gian (`YEAR_OUT_OF_RANGE`)** | 66 | < 0.001% | **Quarantine** (`ctrl.quarantine_log`) | Thời điểm đón nằm ngoài chu kỳ 2023–2024 (do đồng hồ trên xe taxi bị lỗi nhảy về năm 2008, 2009 hoặc 2088). Các chuyến đón đêm 31/12/2024 trả vào sáng 01/01/2025 được chấp nhận. |
| **7** | **Khuyết thiếu bắt buộc (`NULL_REQUIRED`)** | 0 | 0.00% | **Quarantine** (`ctrl.quarantine_log`) | Giá trị NULL tại các cột khóa chính hoặc cột định danh (`pickup_datetime`, `vendor_id`, `locations`, `payment_type`). Tập dữ liệu Parquet chuẩn không có NULL ở các cột lõi. |
| **8** | **Số hành khách bằng 0 (`passenger_count = 0`)** | 1,294,870 | 6.50% | **Chuẩn hóa (Keep & Impute)** | Lỗi cảm biến ghế phụ hoặc tài xế không nhập số khách. Theo quy ước nhóm (Mục 1 Conflict 1), giữ lại bản ghi và ánh xạ vào `passenger_group_key = 4` (Chưa xác định). |
| **9** | **Nhà cung cấp lạ (`VendorID NOT IN (1, 2)`)** | 2,514 | 0.01% | **Chuẩn hóa (Map to OTHER)** | Dữ liệu xuất hiện `VendorID = 6`. Chuẩn hóa ánh xạ sang `vendor_key` ứng với mã `99` (OTHER Vendor) trong bảng `dds.dim_vendor`, không loại bỏ. |
| **10** | **Biểu cước thiếu (`RatecodeID NULL hoặc 99`)** | 180,003 | 0.90% | **Chuẩn hóa (Default 99)** | Gán mặc định vào `rate_code_key = 99` (Special/Unknown Rate) trong `dds.dim_ratecode` để bảo toàn dữ liệu giao dịch. |
| **11** | **Sai lệch cấu trúc (`SCHEMA_DRIFT`)** | Toàn bộ 2023-2024 | 100% | **Xử lý tại Ingestion Stage** | Tên cột phụ phí sân bay đổi từ `airport_fee` (2023) sang `Airport_fee` (2024), kèm lệch kiểu dữ liệu `int64` vs `int32`, `double` vs `int64`. Cần chuẩn hóa kiểu ở tầng Extract của Mage. |

---

## 2. Phân tích Chi tiết Lỗi Schema Drift giữa các Năm

Quá trình đối soát cấu trúc Schema của 24 file Parquet (2023–2024) phát hiện các điểm sai lệch cấu trúc hệ thống:

1. **Sai lệch tên cột (Column Casing Drift):**
   * Năm 2023: Tên cột là `airport_fee` (chữ thường toàn bộ).
   * Năm 2024: Tên cột đổi thành `Airport_fee` (viết hoa chữ cái đầu `A`).
   * *Giải pháp xử lý:* Trong pipeline Mage (`extract_parquet_landing.py`), chuẩn hóa toàn bộ tên cột về chữ thường (`lower()`) ngay khi đọc DataFrame từ tệp Parquet.

2. **Sai lệch kiểu dữ liệu (Data Type Drift):**
   * `VendorID`, `PULocationID`, `DOLocationID`: Năm 2023 lưu dưới kiểu `int64`, năm 2024 chuyển sang `int32`.
   * `passenger_count`, `RatecodeID`: Năm 2023 lưu kiểu `double` (do có chứa giá trị NaN), năm 2024 chuyển thành `int64`.
   * `store_and_fwd_flag`: Năm 2023 lưu kiểu `string`, năm 2024 lưu kiểu `large_string` (PyArrow).
   * *Giải pháp xử lý:* Ép kiểu thống nhất (Explicit Type Casting) tại tầng Staging: các cột ID/Count ép về `INTEGER`, các cột tiền tệ và cự ly ép về `DOUBLE PRECISION` trước khi nạp vào bảng `stage.stg_yellow_trips`.

---

## 3. Chiến thuật Phân luồng & Kiểm soát Chất lượng Dữ liệu

Dựa trên kết quả Profiling, nhóm áp dụng kiến trúc **Hai luồng dữ liệu (Dual-Track Data Routing)** tại mô hình dbt `stg_yellow_trips_clean`:

```
                      [Dữ liệu thô từ Stage: stage.stg_yellow_trips]
                                             │
                                             ▼
                          [Kiểm tra 11 Quy tắc Chất lượng]
                                    │                 │
                  (Thỏa mãn toàn bộ)│                 │(Vi phạm ít nhất 1 lỗi)
                                    ▼                 ▼
             [dds.fact_taxitrip]            [ctrl.quarantine_log]
               (Bản ghi sạch 96.5%)            (Bản ghi lỗi 3.5%)
                                                      │
                                                      ▼
                                       Ghi rõ lý do lỗi đầu tiên (error_reason)
                                       và toàn bộ mảng lỗi (all_errors)
```

### Nguyên tắc nghiệp vụ cốt lõi:
1. **Bảo toàn dữ liệu tuyệt đối (Zero Silent Loss):** Không sử dụng lệnh `DELETE` hay lọc bỏ âm thầm. Mọi bản ghi từ Staging đều phải xuất hiện hoặc ở bảng Fact hoặc ở bảng Quarantine.
2. **Khả năng truy vết (Auditability):** Bảng `ctrl.quarantine_log` lưu trữ đầy đủ `batch_id`, `source_file`, `error_reason`, thời gian xảy ra lỗi và bản sao dữ liệu thô dạng JSON (`raw_payload`) để hỗ trợ đội ngũ dữ liệu truy xuất và đối soát khi có khiếu nại.
3. **Tính lũy kế & Idempotency:** Cả `dds.fact_taxitrip` và `ctrl.quarantine_log` đều sử dụng chiến lược `delete+insert` theo `batch_id`. Khi chạy lại một batch hoặc nạp lại một file, dữ liệu cũ tự động được làm sạch và nạp mới, không sinh ra bản ghi trùng lặp.
