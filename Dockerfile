FROM mageai/mageai:latest

WORKDIR /home/src

# Cài đặt thư viện đọc Parquet, xử lý dữ liệu và dbt-postgres
RUN pip install --no-cache-dir \
    dbt-postgres==1.8.2 \
    pyarrow>=15.0.0 \
    pandas>=2.1.0 \
    psycopg2-binary>=2.9.9 \
    sqlalchemy>=2.0.0
