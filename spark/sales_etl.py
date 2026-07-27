from spark.reader import read_sales_data

from spark.validation import (
    validate_nulls,
    validate_duplicate_sale_id,
    validate_negative_values
)

from spark.transformations import (
    calculate_total_amount,
    add_processing_timestamp,
    add_partition_columns
)

from spark.writer import write_parquet


def main():

    df = read_sales_data()

    print(f"Original Records : {df.count()}")

    df = validate_nulls(df)
    df = validate_duplicate_sale_id(df)
    df = validate_negative_values(df)

    print(f"Valid Records : {df.count()}")

    df = calculate_total_amount(df)
    df = add_processing_timestamp(df)
    df = add_partition_columns(df)

    write_parquet(df)

    print("Sales ETL completed successfully.")


if __name__ == "__main__":
    main()