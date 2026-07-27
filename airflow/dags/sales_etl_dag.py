from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark.sales_etl import run_sales_etl
from spark.tasks import (
    check_sales_file,
    validate_output,
    load_to_bigquery,
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sales_etl_pipeline",
    description="Daily Sales ETL Pipeline using PySpark, GCS and BigQuery",
    start_date=datetime(2026, 7, 27),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sales", "gcp", "pyspark", "bigquery"],
) as dag:

    # Task 1 - Check if sales.csv exists in GCS
    check_file = PythonOperator(
        task_id="check_sales_file",
        python_callable=check_sales_file,
    )

    # Task 2 - Run PySpark ETL
    run_etl = PythonOperator(
        task_id="run_sales_etl",
        python_callable=run_sales_etl,
    )

    # Task 3 - Validate output files
    validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output,
    )

    # Task 4 - Load data into BigQuery
    load_bq = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
    )

    # Task Dependencies
    check_file >> run_etl >> validate >> load_bq