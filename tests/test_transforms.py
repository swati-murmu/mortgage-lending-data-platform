"""
Unit tests for the Spark transformation logic. Runs on a tiny in-memory DataFrame,
so it's fast and CI-friendly (no cluster needed).

    pytest tests/ -v
"""
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.spark.clean_transform import clean  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


def _sample(spark):
    rows = [
        # activity_year, lei, state, county, tract, loan_type, purpose, occ,
        # loan_amount, property_value, income, credit_score, rate, term, spread, action, denial, sex
        (2023, "LEI00001", "CA", "001", "00001", 1, 1, 1, 400000, 500000, 120, 720, 3.5, 360, 0.2, 1, None, "Male"),
        (2023, "LEI00002", "TX", "002", "00002", 2, 31, 1, 250000, 300000, None, 660, 4.1, 360, 0.5, 3, 4, "Female"),
        (2023, "LEI00003", "FL", "003", "00003", 1, 1, 2, 0, 200000, 90, 700, 3.9, 240, 0.1, 1, None, "Joint"),   # bad loan_amount
        (2023, "LEI00004", "NY", "004", "00004", 3, 2, 1, 300000, 0, 80, -1, 3.2, 180, 0.3, 2, None, "Male"),      # bad credit + zero prop
    ]
    cols = ["activity_year", "lei", "state_code", "county_code", "census_tract",
            "loan_type", "loan_purpose", "occupancy_type", "loan_amount", "property_value",
            "income", "credit_score", "interest_rate", "loan_term", "rate_spread",
            "action_taken", "denial_reason", "derived_sex"]
    return spark.createDataFrame(rows, cols)


def test_drops_invalid_loan_amount(spark):
    out = clean(_sample(spark))
    assert out.filter(out.loan_amount <= 0).count() == 0


def test_sentinel_credit_score_nulled(spark):
    out = clean(_sample(spark))
    # the -1 credit score row should now be null, not -1
    assert out.filter(out.credit_score == -1).count() == 0


def test_approval_flag(spark):
    out = clean(_sample(spark))
    # action_taken 1 and 2 count as approved
    approved = out.filter(out.is_approved == 1).count()
    assert approved == 2


def test_ltv_computed_and_non_negative(spark):
    out = clean(_sample(spark))
    ltvs = [r["loan_to_value"] for r in out.select("loan_to_value").collect()
            if r["loan_to_value"] is not None]
    assert all(v >= 0 for v in ltvs)


def test_income_bucket_handles_null(spark):
    out = clean(_sample(spark))
    buckets = {r["income_bucket"] for r in out.select("income_bucket").collect()}
    assert "unknown" in buckets
