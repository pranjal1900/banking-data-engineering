"""
Banking Data Engineering Platform — Fraud Analytics Engine
==========================================================
Applies rule-based heuristics to flag potentially fraudulent transactions.
This runs after data enrichment and before loading to the warehouse.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum as _sum
from pyspark.sql.window import Window

class FraudDetector:
    def __init__(self):
        # Configuration thresholds for rules
        self.high_amount_threshold = 10000.00
        self.velocity_threshold_count = 5
        self.velocity_window_hours = 1

    def apply_rules(self, enriched_tx_df: DataFrame) -> DataFrame:
        """Applies all fraud rules to the enriched transaction dataframe."""
        df = self._rule_high_amount(enriched_tx_df)
        df = self._rule_risky_merchant(df)
        return df

    def _rule_high_amount(self, df: DataFrame) -> DataFrame:
        """Flags transactions exceeding a static high amount threshold."""
        return df.withColumn(
            "fraud_flag_high_amount",
            when(col("amount") > self.high_amount_threshold, True).otherwise(False)
        )

    def _rule_risky_merchant(self, df: DataFrame) -> DataFrame:
        """Flags transactions at High Risk merchants, typically applied during enrichment."""
        # Note: In a real system, merchant_risk_category would come from the merchant dim
        # We assume it's available in the enriched dataframe or we check merchant type
        return df.withColumn(
            "fraud_flag_risky_merchant",
            when(col("channel") == "Online", True).otherwise(False)  # Simplified example
        )

    def calculate_fraud_score(self, df: DataFrame) -> DataFrame:
        """Aggregates individual flags into an overall fraud score (0-100)."""
        return df.withColumn(
            "fraud_score",
            (when(col("fraud_flag_high_amount"), 50).otherwise(0)) +
            (when(col("fraud_flag_risky_merchant"), 20).otherwise(0))
        ).withColumn(
            "is_fraudulent",
            when(col("fraud_score") >= 50, True).otherwise(False)
        )
