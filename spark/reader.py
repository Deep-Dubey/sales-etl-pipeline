from spark.common import get_spark_session, load_config
from spark.schemas import sales_schema


def read_sales_data():
    """
    Read sales CSV from Google Cloud Storage.
    """

    spark = get_spark_session()
    config = load_config()

    df = (
        spark.read
        .option("header", True)
        .schema(sales_schema)
        .csv(config["input"]["sales_file"])
    )

    print("Sales data read successfully from GCS")

    return df