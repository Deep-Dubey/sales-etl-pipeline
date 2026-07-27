from pyspark.sql import SparkSession
import yaml
import os


def load_config():
    """
    Load configuration from config/config.yaml
    """
    with open("config/config.yaml", "r") as file:
        return yaml.safe_load(file)


def get_spark_session():
    """
    Create and return Spark Session configured for
    Google Cloud Storage.
    """

    config = load_config()

    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master("local[*]")

        # GCS Connector Jar
        .config(
            "spark.jars",
            os.path.abspath("jars/gcs-connector-hadoop3-latest.jar")
        )

        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Register GCS FileSystem
    hadoop_conf.set(
        "fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )

    hadoop_conf.set(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )

    # Use Application Default Credentials
    hadoop_conf.set(
        "google.cloud.auth.type",
        "APPLICATION_DEFAULT"
    )

    # --------------------------------------------
    # Application Default Credential File
    # --------------------------------------------

    credential_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not credential_path:
        credential_path = os.path.join(
            os.environ["APPDATA"],
            "gcloud",
            "application_default_credentials.json"
        )

    print(f"Using Credential File : {credential_path}")

    if os.path.exists(credential_path):

        hadoop_conf.set(
            "google.cloud.auth.application.default.credentials.file",
            credential_path
        )

    else:

        raise FileNotFoundError(
            f"Credential file not found : {credential_path}"
        )

    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 60)
    print("Spark Session Started")
    print(f"Application : {config['spark']['app_name']}")
    print(f"Project ID  : {config['gcp']['project_id']}")
    print(f"Raw Bucket  : {config['storage']['raw_bucket']}")
    print(f"Output Path : {config['output']['parquet_path']}")
    print("=" * 60)

    return spark