from pyspark.sql.functions import (
    col,
    current_timestamp,
    year,
    month,
    to_date
)


def calculate_total_amount(df):
    """
    Calculate total sale amount.
    """

    return df.withColumn(
        "total_amount",
        col("quantity") * col("price")
    )


def add_processing_timestamp(df):
    """
    Add ETL processing timestamp.
    """

    return df.withColumn(
        "processing_timestamp",
        current_timestamp()
    )


def convert_sale_date(df):
    """
    Convert sale_date from string to DateType.
    Invalid dates become NULL.
    """

    return df.withColumn(
        "sale_date",
        to_date(col("sale_date"), "yyyy-MM-dd")
    )


def add_partition_columns(df):
    """
    Create partition columns from sale_date.
    """

    return (
        df.withColumn("sale_year", year(col("sale_date")))
          .withColumn("sale_month", month(col("sale_date")))
    )