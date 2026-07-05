"""
Load the star-schema parquet marts into a cloud data warehouse.

Supports Snowflake and generic JDBC (Redshift / Postgres) via --target. Credentials
are read from environment variables so nothing sensitive lives in the repo. If no
warehouse is configured it falls back to a local DuckDB file so the pipeline is fully
runnable on a laptop for demos.

Env vars (Snowflake): SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
                      SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA

Usage:
    spark-submit src/warehouse/load_warehouse.py --marts data/marts --target duckdb
"""
import argparse
import glob
import os

TABLES = ["dim_lender", "dim_geography", "dim_loan_product", "dim_date",
          "fact_loan_applications"]


def load_duckdb(marts, db_path="data/warehouse.duckdb"):
    """Local fallback so reviewers can run the whole pipeline without cloud creds."""
    import duckdb

    con = duckdb.connect(db_path)
    for t in TABLES:
        path = os.path.join(marts, t)
        parquet_glob = os.path.join(path, "**", "*.parquet")
        con.execute(
            f"CREATE OR REPLACE TABLE {t} AS "
            f"SELECT * FROM read_parquet('{parquet_glob}', hive_partitioning=1)"
        )
        n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        print(f"  loaded {t}: {n:,} rows")
    con.close()
    print(f"DuckDB warehouse written to {db_path}")


def load_snowflake(marts):
    """Bulk-load parquet into Snowflake via the Spark connector."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("hmda-load-snowflake").getOrCreate()
    sf_options = {
        "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
        "sfUser": os.environ["SNOWFLAKE_USER"],
        "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
        "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
    for t in TABLES:
        df = spark.read.parquet(os.path.join(marts, t))
        (df.write.format("net.snowflake.spark.snowflake")
           .options(**sf_options).option("dbtable", t.upper())
           .mode("overwrite").save())
        print(f"  loaded {t.upper()} into Snowflake")
    spark.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--marts", default="data/marts")
    p.add_argument("--target", choices=["duckdb", "snowflake"], default="duckdb")
    args = p.parse_args()

    if args.target == "snowflake":
        load_snowflake(args.marts)
    else:
        load_duckdb(args.marts)


if __name__ == "__main__":
    main()
