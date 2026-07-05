"""
Stage 2 Spark job: build a star schema (dimensions + fact) from the curated dataset.

Outputs warehouse-ready parquet that the loader stages into the cloud warehouse:
  - dim_lender
  - dim_geography
  - dim_loan_product
  - dim_date
  - fact_loan_applications  (surrogate keys -> dimensions)

Usage:
    spark-submit src/spark/build_marts.py --input data/curated --output data/marts
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def build_dimensions(df):
    dim_lender = (df.select("lei").distinct()
                  .withColumn("lender_sk", F.monotonically_increasing_id())
                  .withColumn("lender_name", F.concat(F.lit("Lender "), F.col("lei"))))

    dim_geography = (df.select("state_code", "county_code", "census_tract").distinct()
                     .withColumn("geo_sk", F.monotonically_increasing_id()))

    dim_loan_product = (df.select("loan_type", "loan_type_desc", "loan_purpose", "occupancy_type").distinct()
                        .withColumn("product_sk", F.monotonically_increasing_id())
                        .withColumn("loan_purpose_desc",
                            F.element_at(F.create_map(
                                F.lit(1), F.lit("Home Purchase"),
                                F.lit(2), F.lit("Home Improvement"),
                                F.lit(31), F.lit("Refinance"),
                                F.lit(32), F.lit("Cash-out Refinance")), F.col("loan_purpose"))))

    dim_date = (df.select("activity_year").distinct()
                .withColumn("date_sk", F.col("activity_year"))
                .withColumn("decade", (F.floor(F.col("activity_year") / 10) * 10).cast("int")))

    return dim_lender, dim_geography, dim_loan_product, dim_date


def build_fact(df, dim_lender, dim_geography, dim_loan_product):
    fact = (
        df.join(F.broadcast(dim_lender.select("lei", "lender_sk")), "lei")
          .join(dim_geography.select("state_code", "county_code", "census_tract", "geo_sk"),
                ["state_code", "county_code", "census_tract"])
          .join(F.broadcast(dim_loan_product.select(
                "loan_type", "loan_purpose", "occupancy_type", "product_sk")),
                ["loan_type", "loan_purpose", "occupancy_type"])
          .select(
              F.col("activity_year").alias("date_sk"),
              "lender_sk", "geo_sk", "product_sk",
              "loan_amount", "property_value", "income", "credit_score",
              "interest_rate", "rate_spread", "loan_term",
              "loan_to_value", "dti_proxy",
              "action_taken", "denial_reason",
              "is_approved", "is_denied", "income_bucket",
          )
          .withColumn("loan_sk", F.monotonically_increasing_id())
    )
    return fact


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/curated")
    p.add_argument("--output", default="data/marts")
    args = p.parse_args()

    spark = (SparkSession.builder.appName("hmda-build-marts")
             .config("spark.sql.shuffle.partitions", "200").getOrCreate())

    df = spark.read.parquet(args.input)
    dim_lender, dim_geography, dim_loan_product, dim_date = build_dimensions(df)
    fact = build_fact(df, dim_lender, dim_geography, dim_loan_product)

    out = args.output.rstrip("/")
    dim_lender.write.mode("overwrite").parquet(f"{out}/dim_lender")
    dim_geography.write.mode("overwrite").parquet(f"{out}/dim_geography")
    dim_loan_product.write.mode("overwrite").parquet(f"{out}/dim_loan_product")
    dim_date.write.mode("overwrite").parquet(f"{out}/dim_date")
    fact.write.mode("overwrite").partitionBy("date_sk").parquet(f"{out}/fact_loan_applications")

    print(f"Star schema written to {out}")
    spark.stop()


if __name__ == "__main__":
    main()
