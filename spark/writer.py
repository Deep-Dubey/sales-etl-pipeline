from spark.common import load_config


def write_parquet(df):

    config = load_config()

    output_path = (
        f"gs://{config['storage']['processed_bucket']}/"
        f"{config['output']['folder']}"
    )

    (
        df.write
        .mode("overwrite")
        .partitionBy("sale_year", "sale_month")
        .parquet(output_path)
    )

    print(f"Data written successfully to {output_path}")