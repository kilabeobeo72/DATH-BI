import os
from contextlib import contextmanager

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None


def get_pg_connection():
    """Tạo kết nối PostgreSQL từ biến môi trường."""
    if psycopg2 is None:
        raise RuntimeError("Thư viện 'psycopg2' chưa được cài đặt trong môi trường hiện tại.")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "nyc_taxi_dw"),
        user=os.getenv("POSTGRES_USER", "bi_admin"),
        password=os.getenv("POSTGRES_PASSWORD", "change_me_strong_password"),
    )


@contextmanager
def get_db_cursor(commit=False, dict_cursor=False):
    """Context manager tự động đóng connection & cursor, tùy chọn tự động commit."""
    conn = get_pg_connection()
    cursor_factory = RealDictCursor if dict_cursor else None
    cursor = conn.cursor(cursor_factory=cursor_factory)
    try:
        yield conn, cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()
