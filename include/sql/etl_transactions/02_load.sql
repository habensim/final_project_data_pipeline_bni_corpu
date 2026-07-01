-- 02_load.sql
-- transactions_clean → transactions_sample (upsert berdasarkan transaction_code)

INSERT INTO transactions_sample (
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
FROM transactions_clean
ON CONFLICT (transaction_code) DO UPDATE SET
    transaction_id     = EXCLUDED.transaction_id,
    account_id         = EXCLUDED.account_id,
    customer_id        = EXCLUDED.customer_id,
    branch_id          = EXCLUDED.branch_id,
    channel_id         = EXCLUDED.channel_id,
    transaction_date   = EXCLUDED.transaction_date,
    transaction_at     = EXCLUDED.transaction_at,
    transaction_type   = EXCLUDED.transaction_type,
    amount             = EXCLUDED.amount,
    balance_before     = EXCLUDED.balance_before,
    balance_after      = EXCLUDED.balance_after,
    status             = EXCLUDED.status,
    reference_no       = EXCLUDED.reference_no,
    etl_loaded_at       = NOW();