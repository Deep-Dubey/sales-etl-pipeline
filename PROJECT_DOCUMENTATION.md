# Project Documentation

This document explains the repository without exposing the real project name, cloud identifiers, bucket names, personal names, or credential paths. Sensitive values are replaced with placeholders such as `<project-id>`, `<raw-bucket>`, `<processed-bucket>`, and `<credential-file>`.

## 1. Project Purpose

This repository implements a batch sales data pipeline built around PySpark, Apache Airflow, Google Cloud Storage, and BigQuery.

At a high level, the project does the following:

1. Reads a sales CSV file from cloud object storage.
2. Validates and cleans the raw records.
3. Applies ETL transformations to enrich the dataset.
4. Writes the transformed output as partitioned Parquet files.
5. Loads the processed dataset into a BigQuery table.
6. Orchestrates the full workflow through an Airflow DAG.

The repository is organized so the Spark code handles data processing, the Airflow code handles orchestration, the config file stores environment-specific settings, and the test scripts provide quick validation helpers.

## 2. Repository Layout

### Root folders

`airflow/`
Contains the orchestration layer.

`spark/`
Contains the ETL implementation, utility functions, cloud-load logic, and test/helper scripts.

`config/`
Contains YAML-based runtime configuration.

`data/`
Contains a sample or local reference CSV file.

`tests/`
Reserved for a more formal test suite. It is currently empty.

`scripts/`
Reserved for helper scripts. It is currently empty.

`.github/workflows/`
Reserved for CI/CD workflows. The folder exists, but workflow definitions are not currently present.

`jars/`
Used for external Spark connector JARs, especially the Google Cloud Storage Hadoop connector referenced by the Spark session setup.

`output/`
Likely intended for local output artifacts if the project is run outside cloud storage.

`venv/`
Local virtual environment directory.

### Root files

`requirements.txt`
Lists Python dependencies for Spark, Airflow, GCP clients, and development tools.

`README.md`
Currently empty.

`.gitignore`
Currently empty, which means local artifacts and secrets are not yet being ignored.

## 3. Configuration Layer

The configuration file is `config/config.yaml`.

It defines the following logical sections:

`project`
Stores a project label and environment name.

`gcp`
Stores the Google Cloud project identifier.

`storage`
Stores the raw-input and processed-output bucket names.

`input`
Defines the folder and file name for the incoming sales CSV.

`output`
Defines the destination prefix for transformed output files.

`bigquery`
Defines the destination dataset and table.

`spark`
Defines the Spark application name.

The current code reads this file directly at runtime through a helper in the Spark common module. In practice, this file drives storage locations, BigQuery targets, and the Spark application label.

## 4. Spark Processing Modules

### `spark/common.py`

This file provides shared runtime utilities:

`load_config()`
Reads `config/config.yaml` with `yaml.safe_load` and returns the parsed configuration dictionary.

`get_spark_session()`
Builds a local Spark session with:

1. The application name read from config.
2. `local[*]` as the execution master.
3. A configured GCS connector JAR.
4. Hadoop filesystem settings for the `gs://` scheme.
5. Application Default Credential support.

This module is the infrastructure entry point for the ETL code because every reader or transformation path depends on a configured Spark session.

Important design note:
The current implementation includes a hardcoded local credential file path in code. This is functional for one machine, but it is environment-specific and should eventually be externalized.

### `spark/schemas.py`

This file defines the schema used for reading sales CSV data.

The schema currently includes:

1. `sale_id`
2. `customer_id`
3. `product_id`
4. `quantity`
5. `price`
6. `sale_date`

All fields are currently declared as strings. That makes ingestion flexible, but it also means numeric and date typing are deferred until later or are left implicit.

### `spark/reader.py`

This file is responsible for ingestion.

`read_sales_data()` does the following:

1. Creates or retrieves the Spark session.
2. Loads config values.
3. Builds a `gs://` path to the raw CSV file using the configured bucket, folder, and file name.
4. Reads the CSV with a header and the predefined schema.

The result is a Spark DataFrame representing the raw sales data.

### `spark/validation.py`

This file contains row-level data quality filters.

`validate_nulls(df)`
Drops rows containing null values.

`validate_duplicate_sale_id(df)`
Drops duplicate rows based on `sale_id`.

`validate_negative_values(df)`
Filters out rows where `quantity <= 0` or `price <= 0`.

These functions are applied sequentially during the ETL run.

Important implementation note:
Because `quantity` and `price` are currently read as strings, the correctness of the comparison logic depends on Spark's implicit casting behavior. A stronger design would cast these columns explicitly before validation.

### `spark/transformations.py`

This file contains business transformations.

`calculate_total_amount(df)`
Creates a `total_amount` column by multiplying `quantity * price`.

`add_processing_timestamp(df)`
Adds a processing timestamp column using `current_timestamp()`.

`add_partition_columns(df)`
Derives `sale_year` and `sale_month` from `sale_date`.

These transformations prepare the dataset for partitioned output and downstream analytics.

Important implementation note:
Since `sale_date` is currently defined as a string in the schema, extracting `year()` and `month()` relies on Spark being able to interpret the string as a date value.

### `spark/writer.py`

This file handles the processed output write step.

`write_parquet(df)` does the following:

1. Reads config.
2. Builds a `gs://` destination path for the processed bucket and folder.
3. Writes Parquet in overwrite mode.
4. Partitions the output by `sale_year` and `sale_month`.

This makes the output more efficient for downstream query engines and batch reads.

### `spark/sales_etl.py`

This is the main ETL driver.

The current flow is:

1. Import the data reader.
2. Read sales data into a DataFrame.
3. Print the original record count.
4. Apply null validation.
5. Apply duplicate removal by sale identifier.
6. Apply non-negative filtering for quantity and price.
7. Print the valid record count.
8. Compute total amount.
9. Add processing timestamp.
10. Add partition columns.
11. Write the final DataFrame as partitioned Parquet.
12. Print a completion message.

This file is effectively the business workflow entry point for the Spark part of the project.

Important implementation note:
The file currently defines `main()`, but the Airflow DAG imports `run_sales_etl`. That mismatch suggests the Airflow DAG will fail unless a `run_sales_etl()` function is added or the DAG import is updated.

## 5. BigQuery Load Layer

### `spark/bigquery_loader.py`

This file is responsible for loading processed output into BigQuery.

`load_sales_to_bigquery()` does the following:

1. Loads config.
2. Creates a BigQuery client using the configured project identifier.
3. Builds the destination table reference.
4. Builds a cloud storage URI pointing to the processed Parquet files.
5. Configures a BigQuery load job using Parquet as the source format.
6. Uses `WRITE_TRUNCATE`, meaning each run replaces the table contents.
7. Waits for the job to finish.
8. Prints success information.

This module is the warehouse ingestion stage of the pipeline.

## 6. Task Wrapper Layer

### `spark/tasks.py`

This module contains task-sized functions intended for orchestration.

`check_sales_file()`
Connects to cloud storage and checks whether the expected raw CSV object exists.

`validate_output()`
Checks the processed bucket and verifies that output files exist under the configured output prefix.

`load_to_bigquery()`
Delegates to `load_sales_to_bigquery()`.

Important design note:
This file currently embeds bucket names directly in code rather than reading them from config, which duplicates configuration and reduces portability.

## 7. Airflow Orchestration

### `airflow/dags/sales_etl_dag.py`

This file defines the daily Airflow workflow.

The DAG contains four Python tasks:

1. `check_sales_file`
2. `run_sales_etl`
3. `validate_output`
4. `load_to_bigquery`

Execution order:

1. Confirm the input file exists.
2. Run the Spark ETL pipeline.
3. Verify output artifacts were generated.
4. Load the result into BigQuery.

Other DAG characteristics:

1. Daily schedule.
2. Retry count of 3.
3. Retry delay of 2 minutes.
4. `catchup=False`, so backfill is disabled.

Important implementation note:
The DAG imports `run_sales_etl` from the Spark module, but that function is not present in the current ETL file. That is the main orchestration inconsistency in the repository as it stands.

## 8. Test and Validation Scripts

The `spark/` folder contains several script-like test files.

### `spark/test_config.py`

Loads and prints the parsed YAML config. This is a quick environment check rather than a formal test.

### `spark/test_gcs.py`

Creates a Spark session and attempts to read the sales CSV directly from cloud storage. This validates Spark plus GCS connectivity.

### `spark/test_bigquery.py`

Calls the BigQuery load function directly. This validates whether the warehouse load path works.

### `spark/test_bq.py`

Instantiates a BigQuery client and lists datasets for the configured project. This is a connectivity smoke test.

### `spark/test_spark.py`

Currently empty.

Overall, these are more like execution helpers or smoke tests than unit tests. The dedicated `tests/` folder is present but currently unused.

## 9. Dependency Summary

The project depends on the following main packages:

`pyspark`
Distributed data processing engine.

`apache-airflow`
Workflow orchestration engine.

`google-cloud-storage`
Cloud object storage client.

`google-cloud-bigquery`
BigQuery client.

`pyyaml`
YAML parsing for config.

`pytest`
Testing framework, though not fully used yet.

`black` and `flake8`
Formatting and linting tools.

`pandas`
General-purpose data analysis dependency, not currently central to the core Spark flow.

## 10. End-to-End Data Flow

The logical execution path of the project is:

1. Airflow starts the DAG on its schedule.
2. The first task checks whether the source CSV exists in the raw bucket.
3. The ETL task starts Spark with GCS support.
4. Spark reads the configured CSV file from cloud storage.
5. Validation removes null, duplicate, and invalid-value records.
6. Transformation adds metrics and partition columns.
7. The writer stores the dataset as partitioned Parquet in the processed bucket.
8. A validation task confirms output was written.
9. The final task loads the processed files into BigQuery.

## 11. Security and Privacy Notes

This documentation intentionally hides:

1. The real project name.
2. The actual cloud project identifier.
3. The raw and processed bucket names.
4. Personal names used in code metadata.
5. The local credential file path.

For a safer production setup, sensitive values should not be hardcoded in source code. Preferred approaches include:

1. Environment variables.
2. Secret managers.
3. Airflow connections and variables.
4. Externalized deployment configuration.

## 12. Current Gaps and Improvement Areas

The repository structure is sound, but the current implementation has a few notable gaps:

1. The Airflow DAG imports a Spark function name that does not currently exist.
2. The schema uses strings for quantity, price, and sale date, which weakens validation and transformation safety.
3. Some bucket references are hardcoded in task helpers instead of using config.
4. The credential path is hardcoded to one local machine.
5. `.gitignore` is empty, which is risky for virtual environments, generated artifacts, and secret-bearing files.
6. The root `tests/` folder is empty, and most current tests are smoke scripts rather than automated assertions.
7. CI workflow files are not yet defined even though the GitHub Actions folder exists.

## 13. Recommended Next Steps

If this repository is being hardened into a more production-ready data platform, the most valuable next changes would be:

1. Align the Airflow DAG with the Spark entry function.
2. Type the schema correctly and cast fields explicitly.
3. Remove hardcoded storage names and credential paths from code.
4. Populate `.gitignore` for `venv/`, build output, cache directories, and local credential artifacts.
5. Convert smoke scripts into proper `pytest` test cases.
6. Add CI workflows for linting and tests.

## 14. Short Summary

This repository is a cloud-oriented sales ETL pipeline with:

1. Spark for ingestion, validation, transformation, and Parquet output.
2. Google Cloud Storage for raw and processed data locations.
3. BigQuery for analytical storage.
4. Airflow for orchestration.

The architecture is clear and usable as a foundation, while the main work still needed is around configuration hygiene, typed data handling, test maturity, and orchestration consistency.