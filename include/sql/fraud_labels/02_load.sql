-- 02_load.sql
-- fraud_labels_clean → fraud_labels_sample (upsert berdasarkan transaction_code)

INSERT INTO fraud_labels_sample (
    transaction_id,
    transaction_code,
    is_fraud,
    fraud_type,
    fraud_score,
    flagged_at
)
SELECT
    transaction_id,
    transaction_code,
    is_fraud,
    fraud_type,
    fraud_score,
    flagged_at
FROM fraud_labels_clean
ON CONFLICT (transaction_code) DO UPDATE SET
    transaction_id = EXCLUDED.transaction_id,
    is_fraud       = EXCLUDED.is_fraud,
    fraud_type     = EXCLUDED.fraud_type,
    fraud_score    = EXCLUDED.fraud_score,
    flagged_at     = EXCLUDED.flagged_at,
    etl_loaded_at  = NOW();
