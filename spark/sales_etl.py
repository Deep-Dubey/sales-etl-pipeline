from spark.reader import read_sales_data

from spark.validation import (
    validate_nulls,
    validate_duplicate_sale_id,
    validate_negative_values,
)

from spark.transformations import (
    calculate_total_amount,
    add_processing_timestamp,
    add_partition_columns,
)

from spark.writer import write_parquet


def run_sales_etl():
    """
    Main ETL pipeline.
    This function is called from Airflow's PythonOperator.
    """

    print("=" * 60)
    print("Sales ETL Started")
    print("=" * 60)

    # Read data
    df = read_sales_data()

    print(f"Original Records : {df.count()}")

    # ==========================
    # Data Validation
    # ==========================
    df = validate_nulls(df)
    df = validate_duplicate_sale_id(df)
    df = validate_negative_values(df)

    print(f"Valid Records : {df.count()}")

    # ==========================
    # Data Transformation
    # ==========================
    df = calculate_total_amount(df)
    df = add_processing_timestamp(df)
    df = add_partition_columns(df)

    # ==========================
    # Write Output
    # ==========================
    write_parquet(df)

    print("=" * 60)
    print("Sales ETL Completed Successfully")
    print("=" * 60)


def main():
    """
    Entry point for running locally.
    """
    run_sales_etl()


if __name__ == "__main__":
    main()