"""
dag_etl_dim_date.py
=====================
ETL pipeline: dim_date.csv → stg_dim_date → dim_date_full

Catatan: tabel target dinamai `dim_date_full` (bukan `dim_date`) karena di
shared DB `dbcollab` sudah ada tabel `dim_date` milik tim dengan skema minimal
(date_id, date, day, month, year). `dim_date_full` menambah kolom yang
dibutuhkan dashboard: month_name, quarter, week_of_year, is_weekend, dll.

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_dim_date & dim_date
    extract_load   (@task Python)            : baca CSV → stg_dim_date
    transform      (SQLExecuteQueryOperator) : stg_dim_date → dim_date

Airflow Connection:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "postgres_etl" # <-- ganti dengan koneksi database yang sudah dibuat di airflow
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "dim_date.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_dim_date (
    date_id      INTEGER,
    full_date    VARCHAR(20),
    year         SMALLINT,
    quarter      SMALLINT,
    month        SMALLINT,
    month_name   VARCHAR(20),
    week_of_year SMALLINT,
    day_of_month SMALLINT,
    day_of_week  SMALLINT,
    day_name     VARCHAR(20),
    is_weekend   VARCHAR(10),
    is_holiday   VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_date_full (
    date_id       INTEGER      PRIMARY KEY,
    full_date     DATE,
    year          SMALLINT,
    quarter       SMALLINT,
    month         SMALLINT,
    month_name    VARCHAR(20),
    week_of_year  SMALLINT,
    day_of_month  SMALLINT,
    day_of_week   SMALLINT,
    day_name      VARCHAR(20),
    is_weekend    BOOLEAN,
    is_holiday    BOOLEAN,
    etl_loaded_at TIMESTAMP    DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_dim_date",
    description         = "ETL dim_date.csv → stg_dim_date → dim_date_full",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "dim_date", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/dim_date"],
)
def dag_etl_dim_date():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_dim_date ───────────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_dim_date"))
            c.commit()

        df.to_sql(
            name      = "stg_dim_date",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_dim_date → dim_date ────────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_dim_date()
