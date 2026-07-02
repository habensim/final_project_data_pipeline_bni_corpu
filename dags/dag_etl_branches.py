"""
dag_etl_branches.py
=====================
ETL pipeline: branches.csv → stg_branches → dim_branches

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_branches & dim_branches
    extract_load   (@task Python)            : baca CSV → stg_branches
    transform      (SQLExecuteQueryOperator) : stg_branches → dim_branches

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
    os.path.dirname(__file__), "..", "include", "dataset", "branches.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_branches (
    branch_id     INTEGER,
    branch_code   VARCHAR(20),
    branch_name   VARCHAR(150),
    city          VARCHAR(100),
    province      VARCHAR(100),
    region        VARCHAR(50),
    branch_type   VARCHAR(20),
    open_date     VARCHAR(20),
    is_active     VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_branches (
    branch_id     SMALLINT      PRIMARY KEY,
    branch_code   VARCHAR(20),
    branch_name   VARCHAR(150),
    city          VARCHAR(100),
    province      VARCHAR(100),
    region        VARCHAR(50),
    branch_type   VARCHAR(20),
    open_date     DATE,
    is_active     BOOLEAN,
    etl_loaded_at TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_branches",
    description         = "ETL branches.csv → stg_branches → dim_branches",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "branches", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/branches"],
)
def dag_etl_branches():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_branches ───────────────────────────────────
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
            c.execute(text("TRUNCATE TABLE stg_branches"))
            c.commit()

        df.to_sql(
            name      = "stg_branches",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_branches → dim_branches ────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_branches()
