-- 01_transform.sql
-- stg_fraud_labels (TEXT semua kolom) → fraud_labels_clean (typed)

TRUNCATE TABLE fraud_labels_clean;

INSERT INTO fraud_labels_clean (
    transaction_id,
    transaction_code,
    is_fraud,
    fraud_type,
    fraud_score,
    flagged_at
)
SELECT
    NULLIF(transaction_id, '')::INTEGER,
    NULLIF(transaction_code, ''),
    NULLIF(is_fraud, '')::BOOLEAN,
    NULLIF(fraud_type, ''),
    NULLIF(fraud_score, '')::NUMERIC(5,4),
    NULLIF(flagged_at, '')::TIMESTAMP
FROM stg_fraud_labels
WHERE NULLIF(transaction_code, '') IS NOT NULL
ON CONFLICT (transaction_code) DO NOTHING;
