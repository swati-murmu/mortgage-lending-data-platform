-- Denial rate and average loan size by state and loan product.
-- Demonstrates star-schema joins + window analytics over the fact table.

WITH base AS (
    SELECT
        g.state_code,
        p.loan_type_desc,
        p.loan_purpose_desc,
        f.is_denied,
        f.is_approved,
        f.loan_amount,
        f.loan_to_value
    FROM fact_loan_applications f
    JOIN dim_geography     g ON f.geo_sk     = g.geo_sk
    JOIN dim_loan_product  p ON f.product_sk = p.product_sk
)
SELECT
    state_code,
    loan_type_desc,
    loan_purpose_desc,
    COUNT(*)                                              AS applications,
    ROUND(AVG(is_denied) * 100, 2)                        AS denial_rate_pct,
    ROUND(AVG(loan_amount))                               AS avg_loan_amount,
    ROUND(AVG(loan_to_value), 3)                          AS avg_ltv,
    RANK() OVER (PARTITION BY state_code
                 ORDER BY AVG(is_denied) DESC)            AS denial_rank_in_state
FROM base
GROUP BY state_code, loan_type_desc, loan_purpose_desc
HAVING COUNT(*) > 100
ORDER BY state_code, denial_rate_pct DESC;
