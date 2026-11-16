import os
import psycopg2


def get_pg_connection():
    """Tạo kết nối PostgreSQL từ biến môi trường."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyc_taxi_dw"),
        user=os.getenv("POSTGRES_USER", "bi_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "change_me_strong_password"),
    )
