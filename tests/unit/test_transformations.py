"""
Banking Data Engineering Platform — PySpark Unit Tests
======================================================
Tests the Cleansing, Enrichment, and Quality frameworks using Pytest.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from spark.transformations.cleansing import DataCleanser
from spark.fraud.rules import FraudDetector

@pytest.fixture(scope="session")
def spark():
    """Provides a local Spark session for testing."""
    return SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[2]") \
        .getOrCreate()

def test_cleanser_drops_duplicates(spark):
    """Test that the cleanser correctly removes duplicate transactions."""
    cleanser = DataCleanser(spark)
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    data = [
        ("T1", "A1", "100.00", "2026-07-01 10:00:00"),
        ("T1", "A1", "100.00", "2026-07-01 10:00:00"),  # Duplicate
        ("T2", "A2", "50.00", "2026-07-01 11:00:00")
    ]
    
    df = spark.createDataFrame(data, schema)
    clean_df = cleanser.clean_transactions(df)
    
    assert clean_df.count() == 2
    assert "timestamp" not in clean_df.columns  # Ensure it was renamed to transaction_timestamp
    assert "transaction_timestamp" in clean_df.columns

def test_fraud_detector_flags_high_amount(spark):
    """Test that transactions over the threshold are flagged."""
    fraud = FraudDetector()
    fraud.high_amount_threshold = 1000.00
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("channel", StringType(), True)
    ])
    
    data = [
        ("T1", 500.00, "ATM"),
        ("T2", 1500.00, "Online") # Should be flagged for amount and channel
    ]
    
    df = spark.createDataFrame(data, schema)
    result_df = fraud.apply_rules(df)
    final_df = fraud.calculate_fraud_score(result_df).collect()
    
    for row in final_df:
        if row.transaction_id == "T1":
            assert row.is_fraudulent is False
        elif row.transaction_id == "T2":
            assert row.is_fraudulent is True
            assert row.fraud_score >= 50
