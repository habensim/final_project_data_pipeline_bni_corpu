"""
dag_etl_accounts.py
=====================
ETL pipeline: accounts.csv → stg_accounts → dim_accounts

Task flow:
    create_tables  (SQLExecuteQueryOperator) : DDL stg_accounts & dim_accounts
    extract_load   (@task Python)            : baca CSV → stg_accounts
    transform      (SQLExecuteQueryOperator) : stg_accounts → dim_accounts

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
    os.path.dirname(__file__), "..", "include", "dataset", "accounts.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_accounts (
    account_id     VARCHAR(20),
    account_no     VARCHAR(30),
    account_type   VARCHAR(30),
    product_name   VARCHAR(100),
    currency       VARCHAR(5),
    open_date      VARCHAR(20),
    close_date     VARCHAR(20),
    status         VARCHAR(20),
    interest_rate  VARCHAR(20),
    customer_id    VARCHAR(20),
    branch_id      VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_accounts (
    account_id     INTEGER       PRIMARY KEY,
    account_no     VARCHAR(30),
    account_type   VARCHAR(30),
    product_name   VARCHAR(100),
    currency       VARCHAR(5),
    open_date      DATE,
    close_date     DATE,
    status         VARCHAR(20),
    interest_rate  NUMERIC(5,2),
    customer_id    INTEGER,
    branch_id      SMALLINT,
    etl_loaded_at  TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_accounts",
    description         = "ETL accounts.csv → stg_accounts → dim_accounts",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "accounts", "dim", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/accounts"],
)
def dag_etl_accounts():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_accounts ───────────────────────────────────
    @task()
    def extract_load():
        from airflow.hooks.base import BaseHook

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        # dtype=str: account_no boleh diawali 0, close_date sering kosong
        df = pd.read_csv(SOURCE_FILE, dtype=str)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_accounts"))
            c.commit()

        df.to_sql(
            name      = "stg_accounts",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_accounts → dim_accounts ────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract_load() >> transform


dag_etl_accounts()
