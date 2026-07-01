-- 01_transform.sql
-- transactions_raw (TEXT semua kolom) → transactions_clean (typed)

TRUNCATE TABLE transactions_clean;

INSERT INTO transactions_clean (
    transaction_id,
    transaction_code,
    account_id,
    customer_id,
    branch_id,
    channel_id,
    transaction_date,
    transaction_at,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    status,
    reference_no
)
SELECT
    NULLIF(transaction_id, '')::INTEGER,
    NULLIF(transaction_code, ''),
    NULLIF(account_id, '')::INTEGER,
    NULLIF(customer_id, '')::INTEGER,
    NULLIF(branch_id, '')::SMALLINT,
    NULLIF(channel_id, '')::SMALLINT,
    NULLIF(transaction_date, '')::DATE,
    NULLIF(transaction_at, '')::TIMESTAMP,
    NULLIF(transaction_type, ''),
    NULLIF(amount, '')::NUMERIC(18,2),
    NULLIF(balance_before, '')::NUMERIC(18,2),
    NULLIF(balance_after, '')::NUMERIC(18,2),
    NULLIF(status, ''),
    NULLIF(reference_no, '')
FROM transactions_raw
WHERE NULLIF(transaction_code, '') IS NOT NULL
ON CONFLICT (transaction_code) DO NOTHING;