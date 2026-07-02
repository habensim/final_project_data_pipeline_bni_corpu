-- Transform: stg_dim_date → dim_date_full
-- Cast tipe data, deduplikasi.
-- Target dinamai dim_date_full agar tidak menimpa tabel dim_date milik tim
-- yang sudah ada di shared DB dbcollab (skema minimal).

TRUNCATE TABLE dim_date_full;

INSERT INTO dim_date_full (
    date_id,
    full_date,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    is_weekend,
    is_holiday
)
SELECT DISTINCT ON (date_id)
    date_id,
    NULLIF(full_date, '')::DATE,
    year,
    quarter,
    month,
    month_name,
    week_of_year,
    day_of_month,
    day_of_week,
    day_name,
    CASE WHEN LOWER(is_weekend) = 'true' THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN LOWER(is_holiday) = 'true' THEN TRUE ELSE FALSE END AS is_holiday
FROM stg_dim_date
WHERE date_id IS NOT NULL
ORDER BY date_id;
