import csv
import os
from datetime import date, time, timedelta
from pathlib import Path
from psycopg2.extras import execute_values

from mage_project.custom.db_utils import get_pg_connection

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


VIETNAMESE_DAYS = {
    1: "Thứ Hai",
    2: "Thứ Ba",
    3: "Thứ Tư",
    4: "Thứ Năm",
    5: "Thứ Sáu",
    6: "Thứ Bảy",
    7: "Chủ Nhật",
}


def _nth_weekday_of_month(year: int, month: int, weekday: int, n: int) -> date:
    """Tìm ngày thứ n (1-indexed) của một thứ trong tuần (Mon=0..Sun=6) trong tháng."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday_of_month(year: int, month: int, weekday: int) -> date:
    """Tìm ngày cuối cùng của một thứ trong tuần (Mon=0..Sun=6) trong tháng."""
    next_month = date(year + (month // 12), (month % 12) + 1, 1)
    last_day = next_month - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _observed_fixed_holiday(d: date) -> date:
    """Quy tắc nghỉ bù của liên bang Mỹ / NYC khi ngày lễ cố định rơi vào cuối tuần."""
    if d.weekday() == 5:  # Thứ Bảy -> Thứ Sáu
        return d - timedelta(days=1)
    if d.weekday() == 6:  # Chủ Nhật -> Thứ Hai
        return d + timedelta(days=1)
    return d


def _ny_holidays_for_years(years) -> set:
    """Tập hợp các ngày lễ chính thức tại New York / Mỹ cho các năm nghiên cứu."""
    holidays = set()
    for y in years:
        fixed = [
            date(y, 1, 1),    # New Year's Day
            date(y, 6, 19),   # Juneteenth
            date(y, 7, 4),    # Independence Day
            date(y, 11, 11),  # Veterans Day
            date(y, 12, 25),  # Christmas Day
        ]
        for fd in fixed:
            holidays.add(fd)
            holidays.add(_observed_fixed_holiday(fd))

        holidays.add(_nth_weekday_of_month(y, 1, 0, 3))   # MLK Jr. Day (Mon thứ 3 tháng 1)
        holidays.add(_nth_weekday_of_month(y, 2, 0, 3))   # Presidents' Day (Mon thứ 3 tháng 2)
        holidays.add(_last_weekday_of_month(y, 5, 0))     # Memorial Day (Mon cuối tháng 5)
        holidays.add(_nth_weekday_of_month(y, 9, 0, 1))   # Labor Day (Mon đầu tháng 9)
        holidays.add(_nth_weekday_of_month(y, 10, 0, 2))  # Columbus Day (Mon thứ 2 tháng 10)
        holidays.add(_nth_weekday_of_month(y, 11, 3, 4))  # Thanksgiving (Thu thứ 4 tháng 11)
    return holidays


def _build_dim_date_rows() -> list:
    start_date = date(2023, 1, 1)
    end_date = date(2025, 1, 1)  # Bao gồm 2025-01-01 cho chuyến xuyên đêm 31/12/2024
    holidays = _ny_holidays_for_years({2023, 2024, 2025})

    rows = []
    cur = start_date
    while cur <= end_date:
        iso_year, iso_week, iso_weekday = cur.isocalendar()
        quarter_num = (cur.month - 1) // 3 + 1
        rows.append((
            int(cur.strftime("%Y%m%d")),
            cur,
            iso_weekday,
            VIETNAMESE_DAYS[iso_weekday],
            cur.day,
            cur.timetuple().tm_yday,
            iso_week,
            cur.month,
            f"Tháng {cur.month}",
            quarter_num,
            f"Q{quarter_num}",
            cur.year,
            cur.strftime("%Y-%m"),
            1 if iso_weekday >= 6 else 0,
            1 if cur in holidays else 0,
        ))
        cur += timedelta(days=1)
    return rows


def _classify_time_of_day(hour: int) -> str:
    if 6 <= hour <= 10:
        return "Sáng"
    if 11 <= hour <= 13:
        return "Trưa"
    if 14 <= hour <= 17:
        return "Chiều"
    if 18 <= hour <= 21:
        return "Tối"
    return "Đêm"


def _classify_peak_hour(hour: int) -> str:
    if 7 <= hour <= 9:
        return "Cao điểm sáng"
    if 10 <= hour <= 15:
        return "Giờ hành chính"
    if 16 <= hour <= 19:
        return "Cao điểm chiều"
    if 20 <= hour <= 22:
        return "Buổi tối"
    return "Thấp điểm đêm"


def _build_dim_time_rows() -> list:
    rows = []
    for h in range(24):
        for m in range(60):
            hour_12 = 12 if h % 12 == 0 else h % 12
            am_pm = "AM" if h < 12 else "PM"
            rows.append((
                h * 100 + m,
                time(h, m, 0),
                h,
                m,
                hour_12,
                am_pm,
                _classify_time_of_day(h),
                _classify_peak_hour(h),
            ))
    return rows


def _build_dim_location_rows(lookup_dir: str) -> list:
    csv_path = Path(lookup_dir) / "taxi_zone_lookup.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file danh mục khu vực: {csv_path}")

    rows = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for rec in reader:
            loc_id = int(rec["LocationID"])
            zone_raw = (rec.get("Zone") or "").strip()
            borough_raw = (rec.get("Borough") or "").strip()
            service_raw = (rec.get("service_zone") or "").strip()

            zone_name = "Unknown" if zone_raw in ("", "N/A") else zone_raw
            borough = "Unknown" if borough_raw in ("", "N/A") else borough_raw
            service_zone = "Unknown" if service_raw in ("", "N/A") else service_raw

            rows.append((loc_id, loc_id, zone_name, borough, service_zone))
    return rows


def _upsert(cur, sql: str, rows: list):
    execute_values(cur, sql, rows, page_size=1000)


@transformer
def load_static_dims(*args, **kwargs):
    """
    Pipeline etl_load_dimensions (Thành viên 1 - Đào Trung Kiên):
    Sinh và nạp (upsert idempotent) đầy đủ 8 bảng Dimension vào schema dds:
      - dds.dim_date (732 ngày: 2023-01-01 -> 2025-01-01)
      - dds.dim_time (1440 phút)
      - dds.dim_location (265 khu vực từ taxi_zone_lookup.csv)
      - dds.dim_vendor (3 dòng: 1, 2, 99-OTHER)
      - dds.dim_paymenttype (7 dòng: 0..6)
      - dds.dim_ratecode (7 dòng: 1..6, 99)
      - dds.dim_passengergroup (4 dòng: 1..4)
      - dds.dim_distanceband (5 dòng: BAND_1..BAND_5)
    """
    lookup_dir = os.getenv("DATA_LOOKUP_DIR", "/home/src/data/lookup")

    dim_date_rows = _build_dim_date_rows()
    dim_time_rows = _build_dim_time_rows()
    dim_location_rows = _build_dim_location_rows(lookup_dir)

    dim_vendor_rows = [
        (1, 1, "Creative Mobile Technologies LLC", "CMT", "Đang hoạt động"),
        (2, 2, "VeriFone Inc", "VTS", "Đang hoạt động"),
        (3, 99, "Nhà cung cấp khác", "OTHER", "Đang hoạt động"),
    ]

    dim_paymenttype_rows = [
        (1, 0, "Flex Fare / Khác", "Khác", 0),
        (2, 1, "Thẻ tín dụng", "Điện tử", 1),
        (3, 2, "Tiền mặt", "Tiền mặt", 0),
        (4, 3, "Miễn cước", "Miễn phí", 0),
        (5, 4, "Tranh chấp", "Khác", 0),
        (6, 5, "Không xác định", "Khác", 0),
        (7, 6, "Hủy chuyến", "Khác", 0),
    ]

    dim_ratecode_rows = [
        (1, 1, "Cước chuẩn đồng hồ", "Theo đồng hồ", "Giá cước tiêu chuẩn tính theo đồng hồ trong nội đô NYC"),
        (2, 2, "Sân bay JFK", "Trọn gói cố định", "Cước trọn gói cố định tuyến sân bay quốc tế JFK"),
        (3, 3, "Sân bay Newark", "Theo đồng hồ", "Cước đi sân bay quốc tế Newark Liberty kèm phụ phí"),
        (4, 4, "Ngoại ô Nassau Westchester", "Theo đồng hồ", "Cước di chuyển ra khu vực ngoại ô Nassau hoặc Westchester"),
        (5, 5, "Cước thỏa thuận", "Thỏa thuận", "Giá cước thỏa thuận trực tiếp giữa hành khách và tài xế"),
        (6, 6, "Chuyến đi chung", "Theo đồng hồ", "Biểu cước áp dụng cho hình thức đi chung xe"),
        (7, 99, "Cước đặc biệt", "Đặc biệt", "Cước đặc biệt, Flex Fare hoặc không xác định trong dữ liệu gốc"),
    ]

    dim_passengergroup_rows = [
        (1, "1 khách", "Đi một mình", 1, 1, "Hành khách di chuyển cá nhân đơn lẻ"),
        (2, "2 đến 4 khách", "Nhóm nhỏ", 2, 4, "Nhóm gia đình hoặc bạn bè đi xe tiêu chuẩn 4 chỗ"),
        (3, "Từ 5 khách trở lên", "Nhóm lớn", 5, 9, "Nhóm đông người sử dụng xe taxi cỡ lớn hoặc minivan"),
        (4, "Không xác định", "Chưa xác định", 0, 0, "Chuyến xe không ghi nhận số khách (0 hoặc NULL)"),
    ]

    dim_distanceband_rows = [
        (1, "BAND_1", "Rất ngắn", 0.00, 0.99, "Nội phường"),
        (2, "BAND_2", "Ngắn", 1.00, 2.99, "Cận phường"),
        (3, "BAND_3", "Trung bình", 3.00, 6.99, "Nội quận"),
        (4, "BAND_4", "Dài", 7.00, 14.99, "Liên quận"),
        (5, "BAND_5", "Rất dài", 15.00, 9999.99, "Ngoại thành sân bay"),
    ]

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                _upsert(cur, """
                    INSERT INTO dds.dim_date (
                        date_key, full_date, day_number_of_week, day_name_of_week,
                        day_number_of_month, day_number_of_year, week_number_of_year,
                        month_number, month_name, quarter_number, quarter_name,
                        year_number, year_month, is_weekend, is_holiday
                    ) VALUES %s
                    ON CONFLICT (date_key) DO UPDATE SET
                        full_date = EXCLUDED.full_date,
                        day_number_of_week = EXCLUDED.day_number_of_week,
                        day_name_of_week = EXCLUDED.day_name_of_week,
                        day_number_of_month = EXCLUDED.day_number_of_month,
                        day_number_of_year = EXCLUDED.day_number_of_year,
                        week_number_of_year = EXCLUDED.week_number_of_year,
                        month_number = EXCLUDED.month_number,
                        month_name = EXCLUDED.month_name,
                        quarter_number = EXCLUDED.quarter_number,
                        quarter_name = EXCLUDED.quarter_name,
                        year_number = EXCLUDED.year_number,
                        year_month = EXCLUDED.year_month,
                        is_weekend = EXCLUDED.is_weekend,
                        is_holiday = EXCLUDED.is_holiday;
                """, dim_date_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_time (
                        time_key, time_value, hour_24, minute_of_hour,
                        hour_12, am_pm, time_of_day, peak_hour_category
                    ) VALUES %s
                    ON CONFLICT (time_key) DO UPDATE SET
                        time_value = EXCLUDED.time_value,
                        hour_24 = EXCLUDED.hour_24,
                        minute_of_hour = EXCLUDED.minute_of_hour,
                        hour_12 = EXCLUDED.hour_12,
                        am_pm = EXCLUDED.am_pm,
                        time_of_day = EXCLUDED.time_of_day,
                        peak_hour_category = EXCLUDED.peak_hour_category;
                """, dim_time_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_location (
                        location_key, location_id, zone_name, borough, service_zone
                    ) VALUES %s
                    ON CONFLICT (location_key) DO UPDATE SET
                        location_id = EXCLUDED.location_id,
                        zone_name = EXCLUDED.zone_name,
                        borough = EXCLUDED.borough,
                        service_zone = EXCLUDED.service_zone;
                """, dim_location_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_vendor (
                        vendor_key, vendor_id, vendor_name, vendor_abbreviation, vendor_status
                    ) VALUES %s
                    ON CONFLICT (vendor_key) DO UPDATE SET
                        vendor_id = EXCLUDED.vendor_id,
                        vendor_name = EXCLUDED.vendor_name,
                        vendor_abbreviation = EXCLUDED.vendor_abbreviation,
                        vendor_status = EXCLUDED.vendor_status;
                """, dim_vendor_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_paymenttype (
                        payment_type_key, payment_type_id, payment_type_name, payment_category, allows_tip
                    ) VALUES %s
                    ON CONFLICT (payment_type_key) DO UPDATE SET
                        payment_type_id = EXCLUDED.payment_type_id,
                        payment_type_name = EXCLUDED.payment_type_name,
                        payment_category = EXCLUDED.payment_category,
                        allows_tip = EXCLUDED.allows_tip;
                """, dim_paymenttype_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_ratecode (
                        rate_code_key, rate_code_id, rate_code_name, rate_type, description
                    ) VALUES %s
                    ON CONFLICT (rate_code_key) DO UPDATE SET
                        rate_code_id = EXCLUDED.rate_code_id,
                        rate_code_name = EXCLUDED.rate_code_name,
                        rate_type = EXCLUDED.rate_type,
                        description = EXCLUDED.description;
                """, dim_ratecode_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_passengergroup (
                        passenger_group_key, passenger_count_range, passenger_group_name,
                        min_passenger, max_passenger, description
                    ) VALUES %s
                    ON CONFLICT (passenger_group_key) DO UPDATE SET
                        passenger_count_range = EXCLUDED.passenger_count_range,
                        passenger_group_name = EXCLUDED.passenger_group_name,
                        min_passenger = EXCLUDED.min_passenger,
                        max_passenger = EXCLUDED.max_passenger,
                        description = EXCLUDED.description;
                """, dim_passengergroup_rows)

                _upsert(cur, """
                    INSERT INTO dds.dim_distanceband (
                        distance_band_key, distance_band_code, distance_band_name,
                        min_distance_miles, max_distance_miles, trip_type
                    ) VALUES %s
                    ON CONFLICT (distance_band_key) DO UPDATE SET
                        distance_band_code = EXCLUDED.distance_band_code,
                        distance_band_name = EXCLUDED.distance_band_name,
                        min_distance_miles = EXCLUDED.min_distance_miles,
                        max_distance_miles = EXCLUDED.max_distance_miles,
                        trip_type = EXCLUDED.trip_type;
                """, dim_distanceband_rows)

                # Kiểm tra lại số dòng thực tế trong DB
                tables = [
                    "dim_date",
                    "dim_time",
                    "dim_location",
                    "dim_vendor",
                    "dim_paymenttype",
                    "dim_ratecode",
                    "dim_passengergroup",
                    "dim_distanceband",
                ]
                counts = {}
                for tbl in tables:
                    cur.execute(f"SELECT COUNT(*) FROM dds.{tbl};")
                    counts[tbl] = cur.fetchone()[0]
        return {"status": "SUCCESS", "dimension_counts": counts}
    finally:
        conn.close()
