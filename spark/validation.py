from pyspark.sql.functions import col


def validate_nulls(df):
    """
    Remove rows having NULL values
    """
    return df.dropna()


def validate_duplicate_sale_id(df):
    """
    Remove duplicate sale_id
    """
    return df.dropDuplicates(["sale_id"])


def validate_negative_values(df):
    """
    Keep only rows having positive quantity and price
    """
    return df.filter(
        (col("quantity") > 0) &
        (col("price") > 0)
    )