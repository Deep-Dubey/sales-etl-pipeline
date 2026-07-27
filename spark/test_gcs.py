from spark.common import get_spark_session
from spark.common import load_config

spark = get_spark_session()
config = load_config()

df = (
    spark.read
    .option("header", True)
    .csv(
        f"gs://{config['storage']['raw_bucket']}/"
        f"{config['input']['folder']}/"
        f"{config['input']['file_name']}"
    )
)

df.show()