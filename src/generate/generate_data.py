"""
Scalable synthetic HMDA-style mortgage data generator.

Uses Spark to generate an arbitrary number of loan-application records that mirror
the public HMDA Loan Application Register (LAR) schema. Generating with Spark (rather
than pandas) means it scales to hundreds of millions of rows across a cluster.

Usage:
    spark-submit src/generate/generate_data.py --rows 20000000 --output data/raw

To point at the *real* public data instead, see src/ingest/download_hmda.py.
"""
import argparse

from pyspark.sql import SparkSession, functions as F


def build_spark(app_name: str = "hmda-generate") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def generate(spark: SparkSession, rows: int, years):
    """Generate `rows` synthetic loan applications spread across `years`."""
    df = spark.range(0, rows).withColumnRenamed("id", "row_id")

    # Reproducible pseudo-random columns derived from row_id + rand seeds.
    df = (
        df
        .withColumn("activity_year",
                    F.element_at(F.array([F.lit(y) for y in years]),
                                 (F.col("row_id") % F.lit(len(years))).cast("int") + 1))
        # Lender identifier (LEI) — ~5,000 distinct lenders, Zipf-ish skew via modulo.
        .withColumn("lei", F.concat(F.lit("LEI"),
                    F.lpad(((F.col("row_id") * F.lit(2654435761)) % F.lit(5000)).cast("string"), 5, "0")))
        .withColumn("state_code",
                    F.element_at(F.array([F.lit(s) for s in
                        ["CA","TX","FL","NY","AZ","WA","GA","NC","IL","OH","PA","MI","NJ","VA","CO"]]),
                        (F.col("row_id") % F.lit(15)).cast("int") + 1))
        .withColumn("county_code", F.lpad(((F.col("row_id") * 48271) % 300).cast("string"), 3, "0"))
        .withColumn("census_tract", F.lpad(((F.col("row_id") * 40693) % 99999).cast("string"), 5, "0"))
        .withColumn("loan_type", (F.col("row_id") % 4 + 1).cast("int"))          # 1 conv, 2 FHA, 3 VA, 4 USDA
        .withColumn("loan_purpose",
                    F.element_at(F.array([F.lit(x) for x in [1, 2, 31, 32]]),
                                 (F.col("row_id") % 4).cast("int") + 1))
        .withColumn("occupancy_type", (F.col("row_id") % 3 + 1).cast("int"))     # 1 principal, 2 second, 3 investment
        .withColumn("loan_amount", (F.round(F.rand(11) * 950 + 50) * 1000).cast("long"))  # 50k–1M
        .withColumn("property_value", (F.round(F.rand(12) * 1400 + 60) * 1000).cast("long"))
        .withColumn("income", (F.round(F.rand(13) * 240 + 20)).cast("int"))       # in thousands
        .withColumn("credit_score", (F.round(F.rand(14) * 300 + 500)).cast("int"))  # 500–800
        .withColumn("interest_rate", F.round(F.rand(15) * 5 + 2.5, 3))
        .withColumn("loan_term", F.element_at(F.array([F.lit(x) for x in [180, 240, 360]]),
                                              (F.col("row_id") % 3).cast("int") + 1))
        .withColumn("rate_spread", F.round(F.rand(16) * 3 - 0.5, 3))
        # action_taken: 1 originated, 2 approved-not-accepted, 3 denied, 4 withdrawn, 5 incomplete
        .withColumn("action_taken",
                    F.when(F.rand(17) < 0.62, F.lit(1))
                     .when(F.rand(17) < 0.72, F.lit(2))
                     .when(F.rand(17) < 0.85, F.lit(3))
                     .when(F.rand(17) < 0.93, F.lit(4))
                     .otherwise(F.lit(5)))
        .withColumn("denial_reason",
                    F.when(F.col("action_taken") == 3, (F.col("row_id") % 9 + 1).cast("int"))
                     .otherwise(F.lit(None).cast("int")))
        .withColumn("derived_sex", F.element_at(F.array([F.lit(x) for x in ["Male","Female","Joint","NA"]]),
                                                (F.col("row_id") % 4).cast("int") + 1))
    )

    # Inject realistic dirtiness so the cleaning stage has something to do.
    df = (
        df
        .withColumn("income", F.when(F.rand(21) < 0.03, F.lit(None).cast("int")).otherwise(F.col("income")))
        .withColumn("property_value",
                    F.when(F.rand(22) < 0.02, F.lit(None).cast("long")).otherwise(F.col("property_value")))
        .withColumn("credit_score",
                    F.when(F.rand(23) < 0.01, F.lit(-1)).otherwise(F.col("credit_score")))  # sentinel bad value
    )
    return df.drop("row_id")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=20_000_000)
    p.add_argument("--output", default="data/raw")
    p.add_argument("--years", default="2021,2022,2023,2024")
    args = p.parse_args()

    years = [int(y) for y in args.years.split(",")]
    spark = build_spark()
    df = generate(spark, args.rows, years)

    (df.write.mode("overwrite")
       .partitionBy("activity_year")
       .parquet(args.output))

    print(f"Wrote ~{args.rows:,} rows to {args.output} partitioned by activity_year")
    spark.stop()


if __name__ == "__main__":
    main()
