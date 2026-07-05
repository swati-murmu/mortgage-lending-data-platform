"""
Data-quality gate run after the marts are built. Fails (non-zero exit) if any
assertion is violated so Airflow marks the run red and downstream loads are blocked.

Usage:
    spark-submit src/spark/data_quality.py --marts data/marts
"""
import argparse
import sys

from pyspark.sql import SparkSession, functions as F


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--marts", default="data/marts")
    args = p.parse_args()

    spark = SparkSession.builder.appName("hmda-data-quality").getOrCreate()
    fact = spark.read.parquet(f"{args.marts}/fact_loan_applications")

    failures = []

    # 1. No null surrogate keys.
    for k in ["lender_sk", "geo_sk", "product_sk", "date_sk"]:
        n = fact.filter(F.col(k).isNull()).count()
        if n:
            failures.append(f"{n} null values in {k}")

    # 2. approved + denied flags are mutually exclusive.
    both = fact.filter((F.col("is_approved") == 1) & (F.col("is_denied") == 1)).count()
    if both:
        failures.append(f"{both} rows flagged approved AND denied")

    # 3. LTV sanity: no negative loan-to-value.
    bad_ltv = fact.filter(F.col("loan_to_value") < 0).count()
    if bad_ltv:
        failures.append(f"{bad_ltv} rows with negative loan_to_value")

    # 4. Fact is non-empty.
    total = fact.count()
    if total == 0:
        failures.append("fact_loan_applications is empty")

    print(f"Data-quality check ran over {total:,} fact rows")
    if failures:
        print("FAILED:")
        for f_ in failures:
            print("  -", f_)
        spark.stop()
        sys.exit(1)

    print("All data-quality checks PASSED")
    spark.stop()


if __name__ == "__main__":
    main()
