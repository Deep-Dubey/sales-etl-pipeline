from spark.common import get_spark_session
from spark.common import load_config

print("Step 1: Creating Spark Session...")

spark = get_spark_session()

print("Step 2: Loading Config...")

config = load_config()

print("Step 3: Input Path")

print(config["input"]["sales_file"])

# ---------------------------------------
# Debug Hadoop Configuration
# ---------------------------------------

conf = spark.sparkContext._jsc.hadoopConfiguration()

print("Auth Type :", conf.get("google.cloud.auth.type"))

print("FS GS Impl :", conf.get("fs.gs.impl"))

print(
    "AFS GS Impl :",
    conf.get("fs.AbstractFileSystem.gs.impl")
)

print(
    "Credential File :",
    conf.get(
        "google.cloud.auth.application.default.credentials.file"
    )
)

# ---------------------------------------

print("Step 4: Reading CSV...")

df = (
    spark.read
    .option("header", True)
    .csv(config["input"]["sales_file"])
)

print("Step 5: CSV Loaded")

print("Step 6: Schema")

df.printSchema()

print("Step 7: Record Count")

print(df.count())

print("Step 8: Display Data")

df.show(truncate=False)

spark.stop()

print("Finished Successfully")