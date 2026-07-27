from pyspark.sql.functions import col, to_date


def validate_nulls(df):
    """
    Remove rows containing NULL values.
    """
    return df.dropna()


def validate_duplicate_sale_id(df):
    """
    Remove duplicate sale_id records.
    """
    return df.dropDuplicates(["sale_id"])


def validate_negative_values(df):
    """
    Keep only records with positive quantity and price.
    """
    return df.filter(
        (col("quantity") > 0) &
        (col("price") > 0)
    )


def validate_sale_date(df):
    """
    Convert sale_date to DateType and remove invalid dates.
    """

    df = df.withColumn(
        "sale_date",
        to_date(col("sale_date"), "yyyy-MM-dd")
    )

    return df.filter(col("sale_date").isNotNull())