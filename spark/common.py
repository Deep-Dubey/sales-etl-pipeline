from pyspark.sql import SparkSession
import yaml
import os


def load_config():
    """
    Load configuration from config.yaml
    """
    with open("config/config.yaml", "r") as file:
        return yaml.safe_load(file)


def get_spark_session():
    """
    Create and return Spark Session
    """

    config = load_config()

    spark = (
        SparkSession.builder
        .appName(config["spark"]["app_name"])
        .master("local[*]")

        # Google Cloud Storage Connector
        .config(
            "spark.jars",
            os.path.abspath("jars/gcs-connector-hadoop3-latest.jar")
        )

        # Register GCS FileSystem
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )

        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )

        .getOrCreate()
    )

    # Hadoop Configuration
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "google.cloud.auth.type",
        "APPLICATION_DEFAULT"
    )

    credential_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credential_path:
        hadoop_conf.set(
            "google.cloud.auth.application.default.credentials.file",
            credential_path
        )

    spark.sparkContext.setLogLevel("ERROR")

    return spark