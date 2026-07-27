from spark.common import get_spark_session, load_config
from spark.schemas import sales_schema


def read_sales_data():
    spark = get_spark_session()

    config = load_config()

    input_path = (
        f"gs://{config['storage']['raw_bucket']}/"
        f"{config['input']['folder']}/"
        f"{config['input']['file_name']}"
    )

    df = (
        spark.read
        .option("header", True)
        .schema(sales_schema)
        .csv(input_path)
    )

    return df