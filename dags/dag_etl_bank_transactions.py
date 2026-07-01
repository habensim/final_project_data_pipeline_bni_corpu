"""
dag_etl_bank_transactions.py
==============================
ETL pipeline: bank_transactions_data_2.csv → PostgreSQL

Task flow:
    create_tables  (SQLExecuteQueryOperator) : buat tabel staging, clean, final
    extract        (@task Python)            : baca CSV → trx_raw (staging)
    transform      (SQLExecuteQueryOperator) : trx_raw → trx_clean
    load           (SQLExecuteQueryOperator) : trx_clean → trx_sample (upsert)

Airflow Connection yang dibutuhkan:
    conn_id = "postgres_etl"  (tipe: Postgres)
    Host: postgres-etl | Port: 5432 | DB: etl_db

CATATAN PERBAIKAN (lihat komentar "FIX" di bawah untuk detail):
  1. Cek dependency `psycopg2-binary` di requirements.txt — SQLAlchemy engine
     dengan driver "postgresql+psycopg2" akan gagal total (ModuleNotFoundError)
     kalau package ini tidak ter-install. Ini TIDAK bisa saya perbaiki dari kode,
     karena letaknya di requirements.txt, bukan di DAG file ini.
  2. Ditambahkan pool_pre_ping + connect_timeout supaya kalau Postgres belum
     fully-ready saat task start, error yang muncul lebih jelas (bukan silent
     connection reset), dan koneksi lama yang basi tidak dipakai ulang.
  3. Ditambahkan validasi eksplisit untuk keberadaan file CSV, dengan pesan
     error yang menyebutkan path absolut yang dicoba — supaya kalau ini
     penyebabnya, log langsung kasih tahu, bukan generic FileNotFoundError.
  4. Ditambahkan logging di setiap langkah `extract` supaya saat kamu buka
     tab Logs, kamu bisa lihat persis di baris mana proses berhenti.
  5. TRUNCATE dipindah agar hanya jalan setelah CSV berhasil dibaca & tidak
     kosong — supaya kalau baca CSV gagal, tabel raw yang lama tidak ikut
     kehapus sia-sia.
"""

import os
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

log = logging.getLogger(__name__)

# ─── Konstanta ────────────────────────────────────────────────────────────────
CONN_ID     = "postgres_etl"
SOURCE_FILE = os.path.join(
    os.path.dirname(__file__), "..", "include", "dataset", "bank_transactions_data_2.csv"
)

DDL_STATEMENTS = """
CREATE TABLE IF NOT EXISTS trx_raw (
    "TransactionID"           TEXT,
    "AccountID"               TEXT,
    "TransactionAmount"       TEXT,
    "TransactionDate"         TEXT,
    "TransactionType"         TEXT,
    "Location"                TEXT,
    "DeviceID"                TEXT,
    "IP Address"              TEXT,
    "MerchantID"              TEXT,
    "Channel"                 TEXT,
    "CustomerAge"             TEXT,
    "CustomerOccupation"      TEXT,
    "TransactionDuration"     TEXT,
    "LoginAttempts"           TEXT,
    "AccountBalance"          TEXT,
    "PreviousTransactionDate" TEXT
);

CREATE TABLE IF NOT EXISTS trx_clean (
    transaction_id            VARCHAR(20)   PRIMARY KEY,
    account_id                VARCHAR(20),
    transaction_amount        NUMERIC(18,2),
    transaction_date          TIMESTAMP,
    transaction_type          VARCHAR(50),
    location                  VARCHAR(100),
    device_id                 VARCHAR(20),
    ip_address                VARCHAR(45),
    merchant_id               VARCHAR(20),
    channel                   VARCHAR(50),
    customer_age              SMALLINT,
    customer_occupation       VARCHAR(100),
    transaction_duration      INTEGER,
    login_attempts            SMALLINT,
    account_balance           NUMERIC(18,2),
    previous_transaction_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trx_sample (
    transaction_id            VARCHAR(20)   PRIMARY KEY,
    account_id                VARCHAR(20),
    transaction_amount        NUMERIC(18,2),
    transaction_date          TIMESTAMP,
    transaction_type          VARCHAR(50),
    location                  VARCHAR(100),
    device_id                 VARCHAR(20),
    ip_address                VARCHAR(45),
    merchant_id               VARCHAR(20),
    channel                   VARCHAR(50),
    customer_age              SMALLINT,
    customer_occupation       VARCHAR(100),
    transaction_duration      INTEGER,
    login_attempts            SMALLINT,
    account_balance           NUMERIC(18,2),
    previous_transaction_date TIMESTAMP,
    etl_loaded_at             TIMESTAMP     DEFAULT NOW()
);
"""


# ─── DAG ──────────────────────────────────────────────────────────────────────
@dag(
    dag_id              = "dag_etl_bank_transactions",
    description         = "ETL bank_transactions_data_2.csv → PostgreSQL trx_sample",
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
    template_searchpath = ["/opt/airflow/include/sql/etl_banking"],
)
def dag_etl_bank_transactions():

    # ── Task 1: DDL ───────────────────────────────────────────────────────────
    create_tables = SQLExecuteQueryOperator(
        task_id = "create_tables",
        conn_id = CONN_ID,
        sql     = DDL_STATEMENTS,
    )

    # ── Task 2: Extract CSV → trx_raw ─────────────────────────────────────────
    @task()
    def extract():
        from airflow.hooks.base import BaseHook

        # FIX #3: validasi path eksplisit, jangan biarkan pandas yang
        # melempar FileNotFoundError generik.
        abs_path = os.path.abspath(SOURCE_FILE)
        log.info("Mencari file CSV di: %s", abs_path)
        if not os.path.isfile(abs_path):
            raise FileNotFoundError(
                f"CSV tidak ditemukan di path: {abs_path}. "
                f"Cek apakah dag_etl_bank_transactions.py ada langsung di folder "
                f"'dags/' (bukan sub-folder), dan file dataset ada di "
                f"'include/dataset/bank_transactions_data_2.csv'."
            )

        conn = BaseHook.get_connection(CONN_ID)
        conn_str = (
            f"postgresql+psycopg2://{conn.login}:{conn.password}"
            f"@{conn.host}:{conn.port}/{conn.schema}"
        )

        # FIX #2: pool_pre_ping mengetes koneksi sebelum dipakai (menghindari
        # koneksi basi), dan connect_timeout memastikan kita gagal cepat
        # dengan pesan jelas kalau Postgres belum ready, bukan hang lama.
        engine = create_engine(
            conn_str,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 10},
        )

        try:
            log.info("Membaca CSV...")
            df = pd.read_csv(abs_path)
            log.info("Berhasil baca %d baris, kolom: %s", len(df), list(df.columns))

            if df.empty:
                raise ValueError("CSV terbaca tapi kosong (0 baris). Extract dibatalkan.")

            # FIX #5: TRUNCATE hanya setelah CSV valid, di dalam try yang sama
            with engine.begin() as c:  # engine.begin() auto-commit/rollback
                log.info("Truncate trx_raw...")
                c.execute(text("TRUNCATE TABLE trx_raw"))

            log.info("Menulis ke trx_raw...")
            df.to_sql(
                name      = "trx_raw",
                con       = engine,
                if_exists = "append",
                index     = False,
                method    = "multi",
                chunksize = 1000,
            )
            log.info("Selesai. %d baris dimuat ke trx_raw.", len(df))
            return len(df)

        except OperationalError as e:
            log.error("Gagal konek/eksekusi ke Postgres (%s): %s", CONN_ID, e)
            raise
        except Exception as e:
            log.error("Extract gagal: %s", e)
            raise
        finally:
            engine.dispose()

    # ── Task 3: Transform trx_raw → trx_clean ────────────────────────────────
    transform = SQLExecuteQueryOperator(
        task_id = "transform",
        conn_id = CONN_ID,
        sql     = "01_transform.sql",
    )

    # ── Task 4: Load trx_clean → trx_sample (upsert) ─────────────────────────
    load = SQLExecuteQueryOperator(
        task_id = "load",
        conn_id = CONN_ID,
        sql     = "02_load.sql",
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    create_tables >> extract() >> transform >> load


dag_etl_bank_transactions()