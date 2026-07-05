"""
Stage 1 Spark job: clean + standardize raw HMDA loan-application records.

Reads raw parquet (partitioned by activity_year), applies data-quality rules and
derived features, and writes a curated dataset partitioned by year + state.

Design notes for reviewers:
  - Deduplication on a natural key.
  - Null/sentinel handling for known-dirty columns.
  - Derived analytics fields (LTV, DTI proxy, approval flag, income bucket).
  - Idempotent overwrite so the job is safe to re-run from Airflow.

Usage:
    spark-submit src/spark/clean_transform.py --input data/raw --output data/curated
"""
import argparse

from pyspark.sql import SparkSession, functions as F, Window


VALID_ACTIONS = [1, 2, 3, 4, 5]


def clean(df):
    # 1. Drop rows missing an unusable core field.
    df = df.filter(F.col("loan_amount").isNotNull() & (F.col("loan_amount") > 0))
    df = df.filter(F.col("action_taken").isin(VALID_ACTIONS))

    # 2. Repair sentinel / out-of-range values.
    df = df.withColumn(
        "credit_score",
        F.when((F.col("credit_score") < 300) | (F.col("credit_score") > 850), F.lit(None))
         .otherwise(F.col("credit_score")),
    )

    # 3. Derived features.
    df = (
        df
        .withColumn("loan_to_value",
                    F.when(F.col("property_value") > 0,
                           F.round(F.col("loan_amount") / F.col("property_value"), 4)))
        .withColumn("dti_proxy",
                    F.when(F.col("income") > 0,
                           F.round(F.col("loan_amount") / (F.col("income") * 1000), 4)))
        .withColumn("is_approved", F.col("action_taken").isin([1, 2]).cast("int"))
        .withColumn("is_denied", (F.col("action_taken") == 3).cast("int"))
        .withColumn("income_bucket",
                    F.when(F.col("income").isNull(), "unknown")
                     .when(F.col("income") < 50, "<50k")
                     .when(F.col("income") < 100, "50-100k")
                     .when(F.col("income") < 200, "100-200k")
                     .otherwise("200k+"))
        .withColumn("loan_type_desc",
                    F.element_at(F.create_map(
                        F.lit(1), F.lit("Conventional"),
                        F.lit(2), F.lit("FHA"),
                        F.lit(3), F.lit("VA"),
                        F.lit(4), F.lit("USDA")), F.col("loan_type")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    # 4. Deduplicate on a natural business key, keeping the latest ingested row.
    key = ["activity_year", "lei", "census_tract", "loan_amount", "action_taken"]
    w = Window.partitionBy(*key).orderBy(F.col("ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    return df


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/raw")
    p.add_argument("--output", default="data/curated")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("hmda-clean-transform")
             .config("spark.sql.shuffle.partitions", "200").getOrCreate())

    raw = spark.read.parquet(args.input)
    curated = clean(raw)

    (curated.write.mode("overwrite")
        .partitionBy("activity_year", "state_code")
        .parquet(args.output))

    print(f"Curated dataset written to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
