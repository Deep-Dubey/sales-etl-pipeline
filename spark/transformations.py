from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month
)


def calculate_total_amount(df):
    """
    Calculate Total Sale Amount
    """

    return df.withColumn(
        "total_amount",
        col("quantity") * col("price")
    )


def add_processing_timestamp(df):
    """
    Add ETL Processing Timestamp
    """

    return df.withColumn(
        "processing_timestamp",
        current_timestamp()
    )


def add_partition_columns(df):
    """
    Create Partition Columns
    """

    return (
        df.withColumn("sale_year", year(col("sale_date")))
          .withColumn("sale_month", month(col("sale_date")))
    )