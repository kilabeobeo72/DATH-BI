if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def load_static_dims(*args, **kwargs):
    """
    Pipeline etl_load_dimensions (Thành viên 1 - Kiên):
    - Sinh dữ liệu chuẩn cho 8 bảng Dim:
      Dim_Date (732 dòng: 2023-01-01 -> 2025-01-01), Dim_Time (1440 phút),
      Dim_Location (từ taxi_zone_lookup.csv, chuẩn hóa 264/265 -> 'Unknown'),
      Dim_Vendor, Dim_PaymentType, Dim_RateCode, Dim_PassengerGroup, Dim_DistanceBand.
    """
    # TODO (Thành viên 1 - Kiên): Nạp/Upsert 8 bảng Dimension vào schema dds
    return {"status": "TODO"}
