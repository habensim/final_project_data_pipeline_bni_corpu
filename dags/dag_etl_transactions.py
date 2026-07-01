"""
dag_etl_transactions.py
==============================
ETL pipeline: transactions.csv → PostgreSQL

Task flow:
    create_tables  (SQLExecuteQueryOperator) : buat tabel staging, clean, final
    extract        (@task Python)            : baca CSV → transactions_raw (staging)
    transform      (SQLExecuteQueryOperator) : transactions_raw → transactions_clean
    load           (SQLExecuteQueryOperator) : transactions_clean → transactions_sample (upsert)

Airflow Connection yang dibutuhkan:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db

Catatan penempatan file:
    - DAG ini harus ada langsung di folder 'dags/' (bukan sub-folder)
    - CSV harus ada di 'include/dataset/transactions.csv'
    - SQL file (01_transform.sql, 02_load.sql) harus ada di
      'include/sql/etl_transactions/'
"""

import os
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID      = "postgres_etl"
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
SOURCE_FILE  = os.path.join(AIRFLOW_HOME, "include", "dataset", "transactions.csv")

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS transactions_raw (
    transaction_id     TEXT,
    transaction_code   TEXT,
    account_id         TEXT,
    customer_id        TEXT,
    branch_id          TEXT,
    channel_id         TEXT,
    transaction_date   TEXT,
    transaction_at     TEXT,
    transaction_type   TEXT,
    amount             TEXT,
    balance_before     TEXT,
    balance_after      TEXT,
    status             TEXT,
    reference_no       TEXT
);

CREATE TABLE IF NOT EXISTS transactions_clean (
    transaction_id     INTEGER,
    transaction_code   VARCHAR(20)   PRIMARY KEY,
    account_id         INTEGER,
    customer_id        INTEGER,
    branch_id          SMALLINT,
    channel_id         SMALLINT,
    transaction_date   DATE,
    transaction_at     TIMESTAMP,
    transaction_type   VARCHAR(50),
    amount             NUMERIC(18,2),
    balance_before     NUMERIC(18,2),
    balance_after      NUMERIC(18,2),
    status             VARCHAR(20),
    reference_no       VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS transactions_sample (
    transaction_id     INTEGER,
    transaction_code   VARCHAR(20)   PRIMARY KEY,
    account_id         INTEGER,
    customer_id        INTEGER,
    branch_id          SMALLINT,
    channel_id         SMALLINT,
    transaction_date   DATE,
    transaction_at     TIMESTAMP,
    transaction_type   VARCHAR(50),
    amount             NUMERIC(18,2),
    balance_before     NUMERIC(18,2),
    balance_after      NUMERIC(18,2),
    status             VARCHAR(20),
    reference_no       VARCHAR(50),
    etl_loaded_at      TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_transactions",
    description         = "ETL transactions.csv → PostgreSQL transactions_sample",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "banking", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/etl_transactions"],
)
def dag_etl_transactions():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → transactions_raw ────────────────────────────────
    @task()
    def extract():
        import logging

        from airflow.hooks.base import BaseHook

        logger = logging.getLogger(__name__)

        if not os.path.exists(SOURCE_FILE):
            parent_dir = os.path.dirname(SOURCE_FILE)
            listing = os.listdir(parent_dir) if os.path.isdir(parent_dir) else "folder tidak ada"
            logger.error(f"File tidak ditemukan di: {SOURCE_FILE}")
            logger.error(f"Isi folder {parent_dir}: {listing}")
            raise FileNotFoundError(
                f"CSV tidak ditemukan di path: {SOURCE_FILE}. "
                f"Cek apakah dag_etl_transactions.py ada langsung di folder 'dags/' "
                f"(bukan sub-folder), dan file dataset ada di "
                f"'include/dataset/transactions.csv'."
            )

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE, dtype=str)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE transactions_raw"))
            c.commit()

        df.to_sql(
            name      = "transactions_raw",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform transactions_raw → transactions_clean ──────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Task 4: Load transactions_clean → transactions_sample (upsert) ───────
    load = SQLExecuteQueryOperator(
        task_id = "load",
        conn_id = CONN_ID,
        sql     = "02_load.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract() >> transform >> load


dag_etl_transactions()