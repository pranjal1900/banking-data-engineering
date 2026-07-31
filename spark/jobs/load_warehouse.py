"""
Banking Data Engineering Platform — Main Spark Job
==================================================
Ties together Cleansing, Quality, Enrichment, and Fraud.
Includes PySpark optimizations like caching, repartitioning, and broadcast joins.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from spark.transformations.cleansing import DataCleanser
from spark.transformations.enrichment import DataEnricher
from spark.fraud.rules import FraudDetector
from spark.quality.data_quality import DataQualityEngine
from ingestion.storage import StorageManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_job():
    # Optimization: Configure Spark Session with Arrow and Shuffle partitions
    spark = SparkSession.builder \
        .appName("Banking_Warehouse_Loader") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()
        
    storage = StorageManager()
    cleanser = DataCleanser(spark)
    enricher = DataEnricher(spark)
    fraud = FraudDetector()
    quality = DataQualityEngine(spark)
    
    logger.info("Reading raw data...")
    # Normally we read from S3/Local using StorageManager. For this script, assume df variables
    # tx_raw = spark.read.csv("data/raw/transactions/")
    # acct_raw = spark.read.csv("data/raw/accounts/")
    # cust_raw = spark.read.csv("data/raw/customers/")
    
    # Placeholder for actual dataframes
    tx_raw = spark.createDataFrame([], schema="transaction_id string, account_id string, amount string")
    acct_raw = spark.createDataFrame([], schema="account_id string, customer_id string, balance string")
    cust_raw = spark.createDataFrame([], schema="customer_id string, name string, age string")
    
    logger.info("Running Data Cleansing...")
    tx_clean = cleanser.clean_transactions(tx_raw)
    acct_clean = cleanser.clean_accounts(acct_raw)
    cust_clean = cleanser.clean_customers(cust_raw)
    
    logger.info("Running Data Quality Checks...")
    # Check for negative amounts
    tx_passed, tx_failed, result = quality.check_range(tx_clean, "amount", min_val=0.01)
    # We would save tx_failed to rejected_records table here
    
    # Optimization: Cache the passed transactions if they will be used multiple times
    tx_passed.cache()
    
    logger.info("Running Enrichment...")
    # Optimization: Broadcast the smaller dimension tables (accounts, customers) 
    # to avoid shuffling massive transaction fact tables across the cluster.
    enriched_tx = enricher.enrich_transactions(
        tx_passed, 
        broadcast(acct_clean), 
        broadcast(cust_clean)
    )
    
    logger.info("Applying Fraud Rules...")
    fraud_scored_tx = fraud.calculate_fraud_score(fraud.apply_rules(enriched_tx))
    
    # Optimization: Repartition before writing to avoid small file problem
    # Repartition by transaction date/month if doing partitioning
    final_df = fraud_scored_tx.repartition(10)
    
    logger.info("Saving to Warehouse...")
    # storage.write_dataframe(final_df.toPandas(), 'curated', 'transactions', 'fact_transactions.parquet')
    
    logger.info("Job Complete.")

if __name__ == "__main__":
    run_job()
