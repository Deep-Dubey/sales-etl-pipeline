from spark.common import get_spark_session


def main():
    spark = get_spark_session()

    print("=" * 60)
    print("Spark Session Created Successfully")
    print(f"Spark Version : {spark.version}")
    print(f"Application   : {spark.sparkContext.appName}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()