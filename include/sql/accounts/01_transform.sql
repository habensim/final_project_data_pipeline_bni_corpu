-- Transform: stg_accounts → dim_accounts
-- Cast tipe data, handle close_date kosong, deduplikasi

TRUNCATE TABLE dim_accounts;

INSERT INTO dim_accounts (
    account_id,
    account_no,
    account_type,
    product_name,
    currency,
    open_date,
    close_date,
    status,
    interest_rate,
    customer_id,
    branch_id
)
SELECT DISTINCT ON (account_id)
    NULLIF(account_id, '')::INTEGER,
    account_no,
    account_type,
    product_name,
    currency,
    NULLIF(open_date, '')::DATE,
    NULLIF(close_date, '')::DATE,
    status,
    NULLIF(interest_rate, '')::NUMERIC(5,2),
    NULLIF(customer_id, '')::INTEGER,
    NULLIF(branch_id, '')::SMALLINT
FROM stg_accounts
WHERE NULLIF(account_id, '') IS NOT NULL
ORDER BY account_id;
