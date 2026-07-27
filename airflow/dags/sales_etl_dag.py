from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from spark.sales_etl import run_sales_etl
from spark.tasks import (
    check_sales_file,
    validate_output,
    load_to_bigquery
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2026, 7, 27),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    check_file = PythonOperator(
    task_id="check_sales_file",
    python_callable=check_sales_file,
)

    etl = PythonOperator(
    task_id="run_sales_etl",
    python_callable=run_sales_etl,
)

    validate = PythonOperator(
    task_id="validate_output",
    python_callable=validate_output,
)

    load_bq = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=load_to_bigquery,
)

check_file >> etl >> validate >> load_bq