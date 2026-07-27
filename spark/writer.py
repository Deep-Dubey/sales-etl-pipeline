from spark.common import load_config


def write_parquet(df):
    """
    Write transformed data as partitioned Parquet to GCS.
    """

    config = load_config()

    (
        df.write
        .mode("overwrite")
        .partitionBy("sale_year", "sale_month")
        .parquet(config["output"]["parquet_path"])
    )

    print("Partitioned Parquet written successfully to GCS")