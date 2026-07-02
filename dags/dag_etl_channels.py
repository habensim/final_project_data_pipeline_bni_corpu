"""
dag_etl_channels.py
=====================
ETL pipeline: channels.csv → stg_channels → dim_channels

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_channels & dim_channels
    extract_load   (@task Python)            : baca CSV → stg_channels
    transform      (SQLExecuteQueryOperator) : stg_channels → dim_channels

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
    os.path.dirname(__file__), "..", "include", "dataset", "channels.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_channels (
    channel_id       INTEGER,
    channel_code     VARCHAR(20),
    channel_name     VARCHAR(100),
    channel_category VARCHAR(20),
    is_digital       VARCHAR(10),
    description      VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_channels (
    channel_id       SMALLINT      PRIMARY KEY,
    channel_code     VARCHAR(20),
    channel_name     VARCHAR(100),
    channel_category VARCHAR(20),
    is_digital       BOOLEAN,
    description      VARCHAR(255),
    etl_loaded_at    TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_channels",
    description         = "ETL channels.csv → stg_channels → dim_channels",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "channels", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/channels"],
)
def dag_etl_channels():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_channels ───────────────────────────────────
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
            c.execute(text("TRUNCATE TABLE stg_channels"))
            c.commit()

        df.to_sql(
            name      = "stg_channels",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_channels → dim_channels ────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_channels()
