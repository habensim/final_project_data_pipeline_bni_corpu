"""
dag_etl_fraud_labels.py
==============================
ETL pipeline: fraud_labels.csv → PostgreSQL

Task flow:
    create_tables  (SQLExecuteQueryOperator) : buat tabel staging, clean, final
    extract        (@task Python)            : baca CSV → stg_fraud_labels (staging)
    transform      (SQLExecuteQueryOperator) : stg_fraud_labels → fraud_labels_clean
    load           (SQLExecuteQueryOperator) : fraud_labels_clean → fraud_labels_sample (upsert)

Airflow Connection yang dibutuhkan:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db

Catatan penempatan file:
    - DAG ini harus ada langsung di folder 'dags/' (bukan sub-folder)
    - CSV harus ada di 'include/dataset/fraud_labels.csv'
    - SQL file (01_transform.sql, 02_load.sql) harus ada di
      'include/sql/fraud_labels/'
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
SOURCE_FILE  = os.path.join(AIRFLOW_HOME, "include", "dataset", "fraud_labels.csv")

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS stg_fraud_labels (
    transaction_id      TEXT,
    transaction_code    TEXT,
    is_fraud             TEXT,
    fraud_type           TEXT,
    fraud_score          TEXT,
    flagged_at           TEXT
);

CREATE TABLE IF NOT EXISTS fraud_labels_clean (
    transaction_id      INTEGER,
    transaction_code    VARCHAR(20)   PRIMARY KEY,
    is_fraud             BOOLEAN,
    fraud_type           VARCHAR(50),
    fraud_score          NUMERIC(5,4),
    flagged_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_labels_sample (
    transaction_id      INTEGER,
    transaction_code    VARCHAR(20)   PRIMARY KEY,
    is_fraud             BOOLEAN,
    fraud_type           VARCHAR(50),
    fraud_score          NUMERIC(5,4),
    flagged_at           TIMESTAMP,
    etl_loaded_at         TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_fraud_labels",
    description         = "ETL fraud_labels.csv → PostgreSQL fraud_labels_sample",
    default_args        = {
        "owner"           : "airflow",
        "retries"         : 1,
        "retry_delay"     : timedelta(minutes=5),
        "email_on_failure": False,
    },
    start_date          = datetime(2025, 1, 1),
    schedule            = None,
    catchup             = False,
    tags                = ["etl", "fraud", "banking", "postgresql"],
    template_searchpath = ["/opt/airflow/include/sql/fraud_labels"],
)
def dag_etl_fraud_labels():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → stg_fraud_labels ────────────────────────────────
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
                f"Cek apakah dag_etl_fraud_labels.py ada langsung di folder 'dags/' "
                f"(bukan sub-folder), dan file dataset ada di "
                f"'include/dataset/fraud_labels.csv'."
            )

        conn     = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )
        engine = create_engine(conn_str)

        df = pd.read_csv(SOURCE_FILE, dtype=str)

        with engine.connect() as c:
            c.execute(text("TRUNCATE TABLE stg_fraud_labels"))
            c.commit()

        df.to_sql(
            name      = "stg_fraud_labels",
            con       = engine,
            if_exists = "append",
            index     = False,
            method    = "multi",
            chunksize = 1000,
        )
        engine.dispose()
        return len(df)

    # ── Task 3: Transform stg_fraud_labels → fraud_labels_clean ──────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Task 4: Load fraud_labels_clean → fraud_labels_sample (upsert) ───────
    load = SQLExecuteQueryOperator(
        task_id = "load",
        conn_id = CONN_ID,
        sql     = "02_load.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract() >> transform >> load


dag_etl_fraud_labels()
