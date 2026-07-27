from google.cloud import storage
from spark.common import load_config
from spark.bigquery_loader import load_sales_to_bigquery


def check_sales_file():
    """
    Check whether the input sales file exists in GCS.
    """

    config = load_config()

    client = storage.Client()

    bucket_name = config["storage"]["raw_bucket"]

    # Extract object path from gs://bucket/path
    object_name = config["input"]["sales_file"].replace(
        f"gs://{bucket_name}/", ""
    )

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    if not blob.exists():
        raise FileNotFoundError(f"{object_name} not found.")

    print(f"Input file found: {object_name}")


def validate_output():
    """
    Validate that Spark has written output files to GCS.
    """

    config = load_config()

    client = storage.Client()

    bucket_name = config["storage"]["processed_bucket"]

    output_prefix = config["output"]["parquet_path"].replace(
        f"gs://{bucket_name}/", ""
    )

    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=output_prefix))

    if not blobs:
        raise Exception("No output files found in GCS.")

    print("Output validation successful.")


def load_to_bigquery():
    """
    Load processed Parquet data into BigQuery.
    """

    load_sales_to_bigquery()