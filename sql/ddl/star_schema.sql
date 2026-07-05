-- Star-schema DDL for the mortgage-lending warehouse.
-- Written for Snowflake / Redshift; types are ANSI-friendly and load-compatible
-- with the parquet marts produced by src/spark/build_marts.py.

CREATE TABLE IF NOT EXISTS dim_lender (
    lender_sk      BIGINT       PRIMARY KEY,
    lei            VARCHAR(32)  NOT NULL,
    lender_name    VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geo_sk         BIGINT       PRIMARY KEY,
    state_code     VARCHAR(2)   NOT NULL,
    county_code    VARCHAR(3),
    census_tract   VARCHAR(11)
);

CREATE TABLE IF NOT EXISTS dim_loan_product (
    product_sk         BIGINT      PRIMARY KEY,
    loan_type          SMALLINT,
    loan_type_desc     VARCHAR(32),
    loan_purpose       SMALLINT,
    loan_purpose_desc  VARCHAR(32),
    occupancy_type     SMALLINT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_sk        INT          PRIMARY KEY,
    activity_year  INT          NOT NULL,
    decade         INT
);

CREATE TABLE IF NOT EXISTS fact_loan_applications (
    loan_sk         BIGINT      PRIMARY KEY,
    date_sk         INT         NOT NULL REFERENCES dim_date(date_sk),
    lender_sk       BIGINT      NOT NULL REFERENCES dim_lender(lender_sk),
    geo_sk          BIGINT      NOT NULL REFERENCES dim_geography(geo_sk),
    product_sk      BIGINT      NOT NULL REFERENCES dim_loan_product(product_sk),
    loan_amount     BIGINT,
    property_value  BIGINT,
    income          INT,
    credit_score    INT,
    interest_rate   DECIMAL(6,3),
    rate_spread     DECIMAL(6,3),
    loan_term       INT,
    loan_to_value   DECIMAL(8,4),
    dti_proxy       DECIMAL(8,4),
    action_taken    SMALLINT,
    denial_reason   SMALLINT,
    is_approved     SMALLINT,
    is_denied       SMALLINT,
    income_bucket   VARCHAR(16)
);

-- Distribution / clustering hints (Redshift example):
-- ALTER TABLE fact_loan_applications ALTER DISTKEY lender_sk;
-- ALTER TABLE fact_loan_applications ALTER SORTKEY (date_sk);
