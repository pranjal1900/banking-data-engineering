"""
Banking Data Engineering Platform — PySpark Data Cleansing
==========================================================
Handles raw data reading, schema validation, null handling,
and type casting before loading into the warehouse.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, trim, lower, current_timestamp
from pyspark.sql.types import IntegerType, DecimalType, DateType, TimestampType

class DataCleanser:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def clean_customers(self, df: DataFrame) -> DataFrame:
        """Cleans raw customer data."""
        return df \
            .withColumn("name", trim(col("name"))) \
            .withColumn("gender", lower(col("gender"))) \
            .withColumn("age", col("age").cast(IntegerType())) \
            .withColumn("income", col("income").cast(DecimalType(15, 2))) \
            .withColumn("account_open_date", col("account_open_date").cast(DateType())) \
            .filter(col("age") >= 18) \
            .filter(col("customer_id").isNotNull()) \
            .dropDuplicates(["customer_id"])

    def clean_accounts(self, df: DataFrame) -> DataFrame:
        """Cleans raw account data."""
        return df \
            .withColumn("balance", col("balance").cast(DecimalType(15, 2))) \
            .withColumn("account_status", trim(col("account_status"))) \
            .filter(col("account_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .dropDuplicates(["account_id"])

    def clean_transactions(self, df: DataFrame) -> DataFrame:
        """Cleans raw transaction data."""
        return df \
            .withColumn("amount", col("amount").cast(DecimalType(15, 2))) \
            .withColumn("transaction_timestamp", col("timestamp").cast(TimestampType())) \
            .filter(col("transaction_id").isNotNull()) \
            .filter(col("account_id").isNotNull()) \
            .filter(col("amount") > 0) \
            .dropDuplicates(["transaction_id"]) \
            .drop("timestamp")

    def clean_branches(self, df: DataFrame) -> DataFrame:
        """Cleans raw branch data."""
        return df \
            .withColumn("branch_name", trim(col("branch_name"))) \
            .filter(col("branch_id").isNotNull()) \
            .dropDuplicates(["branch_id"])

    def clean_merchants(self, df: DataFrame) -> DataFrame:
        """Cleans raw merchant data."""
        return df \
            .withColumn("merchant_name", trim(col("merchant_name"))) \
            .filter(col("merchant_id").isNotNull()) \
            .dropDuplicates(["merchant_id"])
