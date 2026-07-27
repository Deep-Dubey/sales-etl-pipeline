from google.cloud import bigquery
from spark.common import load_config


def load_sales_to_bigquery():
    """
    Load partitioned Parquet files from GCS into BigQuery.
    """

    config = load_config()

    client = bigquery.Client(
        project=config["gcp"]["project_id"]
    )

    table_id = (
        f"{config['gcp']['project_id']}."
        f"{config['bigquery']['dataset']}."
        f"{config['bigquery']['table']}"
    )

    source_uri = config["output"]["parquet_path"] + "*"

    print(f"Project   : {config['gcp']['project_id']}")
    print(f"Table ID  : {table_id}")
    print(f"Source URI: {source_uri}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_uri(
        source_uri,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    print("BigQuery Load Completed Successfully")