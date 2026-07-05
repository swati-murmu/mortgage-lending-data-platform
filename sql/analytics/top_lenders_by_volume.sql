-- Top 20 lenders by originated volume, with approval rate and YoY growth.
-- Demonstrates multi-dimension joins + LAG window function for time-series growth.

WITH lender_year AS (
    SELECT
        l.lender_name,
        d.activity_year,
        SUM(CASE WHEN f.action_taken = 1 THEN f.loan_amount ELSE 0 END) AS originated_amount,
        ROUND(AVG(f.is_approved) * 100, 2)                              AS approval_rate_pct
    FROM fact_loan_applications f
    JOIN dim_lender l ON f.lender_sk = l.lender_sk
    JOIN dim_date   d ON f.date_sk   = d.date_sk
    GROUP BY l.lender_name, d.activity_year
)
SELECT
    lender_name,
    activity_year,
    originated_amount,
    approval_rate_pct,
    LAG(originated_amount) OVER (PARTITION BY lender_name ORDER BY activity_year) AS prior_year_amount,
    ROUND(
        100.0 * (originated_amount
                 - LAG(originated_amount) OVER (PARTITION BY lender_name ORDER BY activity_year))
        / NULLIF(LAG(originated_amount) OVER (PARTITION BY lender_name ORDER BY activity_year), 0),
    2) AS yoy_growth_pct
FROM lender_year
QUALIFY RANK() OVER (PARTITION BY activity_year ORDER BY originated_amount DESC) <= 20
ORDER BY activity_year DESC, originated_amount DESC;
