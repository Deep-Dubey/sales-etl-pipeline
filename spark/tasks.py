from google.cloud import storage
from spark.common import load_config
from spark.bigquery_loader import load_sales_to_bigquery


def check_sales_file():
    config = load_config()

    client = storage.Client()

    bucket = client.bucket(config["storage"]["raw_bucket"])

    blob = bucket.blob(
        f"{config['input']['folder']}/{config['input']['file_name']}"
    )

    if not blob.exists():
        raise FileNotFoundError("sales.csv not found.")

    print("sales.csv found.")


def validate_output():
    config = load_config()

    client = storage.Client()

    bucket = client.bucket(config["storage"]["processed_bucket"])

    blobs = list(
        bucket.list_blobs(prefix=f"{config['output']['folder']}/")
    )

    if len(blobs) == 0:
        raise Exception("Output not created.")

    print("Output validated.")


def load_to_bigquery():

    load_sales_to_bigquery()