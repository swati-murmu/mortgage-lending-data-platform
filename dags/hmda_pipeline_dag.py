"""
Airflow DAG orchestrating the end-to-end HMDA mortgage-lending pipeline.

    generate/ingest  ->  spark_clean  ->  spark_build_marts  ->  data_quality  ->  load_warehouse  ->  refresh_analytics

Each Spark stage is submitted with SparkSubmitOperator (swap for the appropriate
operator on EMR / Dataproc / Databricks in production). The data_quality task is a
hard gate — if it fails, the warehouse load never runs.

Schedule: daily. Backfill-safe because every Spark stage writes with overwrite
semantics keyed on partitions.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

try:
    from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
    HAVE_SPARK_OPERATOR = True
except ImportError:  # keep the DAG importable even without the provider installed
    HAVE_SPARK_OPERATOR = False

PROJECT = "/opt/airflow/project"
ROWS = "{{ var.value.get('hmda_rows', 20000000) }}"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=3),
}


def spark_task(dag, task_id, script, args):
    """Return a Spark task, falling back to a bash spark-submit if the provider is absent."""
    app = f"{PROJECT}/{script}"
    if HAVE_SPARK_OPERATOR:
        return SparkSubmitOperator(
            task_id=task_id, application=app, application_args=args,
            conf={"spark.sql.shuffle.partitions": "200"},
            executor_memory="4g", num_executors="4", dag=dag,
        )
    cmd = f"spark-submit {app} " + " ".join(args)
    return BashOperator(task_id=task_id, bash_command=cmd, dag=dag)


with DAG(
    dag_id="hmda_mortgage_lending_pipeline",
    description="Large-scale mortgage lending analytics: Spark + warehouse star schema",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spark", "mortgage", "warehouse", "portfolio"],
) as dag:

    generate = spark_task(
        dag, "generate_data", "src/generate/generate_data.py",
        ["--rows", ROWS, "--output", f"{PROJECT}/data/raw"],
    )

    clean = spark_task(
        dag, "spark_clean_transform", "src/spark/clean_transform.py",
        ["--input", f"{PROJECT}/data/raw", "--output", f"{PROJECT}/data/curated"],
    )

    marts = spark_task(
        dag, "spark_build_marts", "src/spark/build_marts.py",
        ["--input", f"{PROJECT}/data/curated", "--output", f"{PROJECT}/data/marts"],
    )

    dq = spark_task(
        dag, "data_quality_gate", "src/spark/data_quality.py",
        ["--marts", f"{PROJECT}/data/marts"],
    )

    load = spark_task(
        dag, "load_warehouse", "src/warehouse/load_warehouse.py",
        ["--marts", f"{PROJECT}/data/marts", "--target", "duckdb"],
    )

    refresh_analytics = BashOperator(
        task_id="refresh_analytics",
        bash_command=f"python {PROJECT}/src/warehouse/run_analytics.py",
    )

    generate >> clean >> marts >> dq >> load >> refresh_analytics
