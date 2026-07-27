from google.cloud import bigquery
from spark.common import load_config


def test_bigquery_connection():
    """
    Test BigQuery connection and list all datasets.
    """

    config = load_config()

    client = bigquery.Client(
        project=config["gcp"]["project_id"]
    )

    print("=" * 60)
    print("Connected to BigQuery")
    print(f"Project: {config['gcp']['project_id']}")
    print("Available Datasets")
    print("=" * 60)

    for dataset in client.list_datasets():
        print(dataset.dataset_id)

    print("=" * 60)
    print("BigQuery Connection Successful")
    print("=" * 60)


if __name__ == "__main__":
    test_bigquery_connection()