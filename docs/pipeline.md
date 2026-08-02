# ETL Pipeline Architecture

The ETL pipeline orchestrates the end-to-end movement of data from raw generation to analytical modeling.

## Orchestration (Airflow)
Airflow schedules and monitors the jobs on a `@daily` basis.
- **DAG:** `banking_daily_etl_pipeline`
- **Tasks:**
  1. `extract_raw_data`: Triggers the Python generators.
  2. `process_and_load_warehouse`: Submits the main PySpark job.
  3. `reconciliation_check`: Validates that no data was lost between source and target.

## Processing (PySpark)
The PySpark application (`load_warehouse.py`) is modularized:
1. **Cleansing:** `spark/transformations/cleansing.py` casts data types and drops exact duplicates.
2. **Quality Framework:** `spark/quality/data_quality.py` enforces rules (Completeness, Uniqueness, Range). Bad records are sent to a Dead Letter Queue (DLQ).
3. **Enrichment:** `spark/transformations/enrichment.py` joins the fact table (transactions) to dimension tables (accounts, customers) to denormalize the data for BI.
4. **Fraud Analytics:** `spark/fraud/rules.py` applies business logic rules to calculate a fraud score and sets a boolean flag for analysts.

## Optimizations Used
- **Broadcast Joins:** Small dimension tables (branches, merchants) are broadcasted to all worker nodes to avoid network shuffling when joining with the massive transaction fact table.
- **Incremental Loading (Watermarks):** We query PostgreSQL (`quality.pipeline_watermarks`) to find the maximum processed timestamp for a dataset, ensuring Spark only reads and processes new files.
- **Parquet Storage:** Data is stored in columnar Parquet format rather than CSV, reducing storage footprint and drastically improving read performance.
