"""
Banking Data Engineering Platform — PySpark Data Enrichment
===========================================================
Handles joining cleaned tables to create enriched datasets
and deriving new calculated columns (e.g., transaction day of week,
customer tenure, risk flags).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, datediff, current_date, dayofweek, month, year, when, concat_ws

class DataEnricher:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def enrich_customers(self, df: DataFrame) -> DataFrame:
        """Derives customer tenure in years and risk scoring based on age/income."""
        return df \
            .withColumn("tenure_years", datediff(current_date(), col("account_open_date")) / 365) \
            .withColumn("high_income_flag", when(col("income") > 150000, True).otherwise(False))

    def enrich_transactions(self, tx_df: DataFrame, acct_df: DataFrame, cust_df: DataFrame) -> DataFrame:
        """
        Denormalizes transactions by joining with account and customer data
        for easier downstream analytics. Adds time-based derived columns.
        """
        # Join Transaction -> Account -> Customer
        enriched_df = tx_df.alias("tx") \
            .join(acct_df.alias("ac"), col("tx.account_id") == col("ac.account_id"), "left") \
            .join(cust_df.alias("cu"), col("ac.customer_id") == col("cu.customer_id"), "left")

        # Select necessary columns and add time dimensions
        return enriched_df.select(
            col("tx.transaction_id"),
            col("tx.account_id"),
            col("tx.merchant_id"),
            col("tx.transaction_type"),
            col("tx.amount"),
            col("tx.transaction_timestamp"),
            col("tx.status"),
            col("tx.channel"),
            col("cu.customer_id"),
            col("cu.customer_segment"),
            col("cu.city").alias("customer_city"),
            col("ac.account_type")
        ) \
        .withColumn("tx_year", year(col("transaction_timestamp"))) \
        .withColumn("tx_month", month(col("transaction_timestamp"))) \
        .withColumn("tx_day_of_week", dayofweek(col("transaction_timestamp"))) \
        .withColumn("is_weekend", when(col("tx_day_of_week").isin([1, 7]), True).otherwise(False))
