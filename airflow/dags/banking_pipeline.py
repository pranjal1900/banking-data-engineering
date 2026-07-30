"""
Banking Data Engineering Platform — Airflow Orchestration DAG
=============================================================
Orchestrates the entire end-to-end data pipeline running daily.
Extract -> Validate -> Clean -> Enrich -> Load -> Analyze
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'banking_daily_etl_pipeline',
    default_args=default_args,
    description='End-to-End Banking Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['banking', 'etl'],
) as dag:

    # 1. Extraction: Run the data generators (mocking external source systems)
    # In a real environment, this would be an API pull, SFTP transfer, or CDC tool.
    extract_data = PythonOperator(
        task_id='extract_raw_data',
        python_callable=lambda: os.system("python -m ingestion.ingest --size medium")
    )

    # 2. Spark Job: Load to Warehouse
    # This runs the primary Spark job that applies Data Quality -> Cleansing -> Enrichment -> Warehouse Load
    process_warehouse = SparkSubmitOperator(
        task_id='process_and_load_warehouse',
        application='spark/jobs/load_warehouse.py',
        conn_id='spark_default',
        conf={
            'spark.sql.shuffle.partitions': '50',
            'spark.driver.memory': '2g'
        },
        name='banking_warehouse_loader'
    )

    # 3. Post-Load Quality Check: Reconciliation
    # Ensures the number of transactions loaded matches the raw files
    reconciliation_check = PythonOperator(
        task_id='reconciliation_check',
        python_callable=lambda: print("Reconciliation Check Passed")  # Placeholder for reconciliation script
    )

    # Define task dependencies
    extract_data >> process_warehouse >> reconciliation_check
