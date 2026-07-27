from google.cloud import bigquery
from spark.common import load_config

config = load_config()

client = bigquery.Client(project=config["gcp"]["project_id"])

for dataset in client.list_datasets():
    print(dataset.dataset_id)