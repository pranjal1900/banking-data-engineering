"""
Banking Data Engineering Platform — Data Quality Framework
==========================================================
Evaluates Spark DataFrames against predefined quality rules.
Identifies bad records (for the rejected_records table) and 
calculates pass rates (for the quality_results table).
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, isnull, count, expr, lit
from typing import Dict, List, Tuple

class DataQualityEngine:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def check_completeness(self, df: DataFrame, columns: List[str]) -> Tuple[DataFrame, Dict]:
        """Checks for NULLs in critical columns."""
        total_count = df.count()
        if total_count == 0:
            return df, self._empty_result("COMPLETENESS", columns)

        # Build filter condition: col1 IS NULL OR col2 IS NULL
        conditions = [isnull(col(c)) for c in columns]
        combined_condition = conditions[0]
        for c in conditions[1:]:
            combined_condition = combined_condition | c

        failed_df = df.filter(combined_condition).withColumn("failure_reason", lit("Missing critical values"))
        passed_df = df.filter(~combined_condition)
        
        failed_count = failed_df.count()
        
        result = {
            "check_type": "COMPLETENESS",
            "columns": columns,
            "total_records": total_count,
            "passed_records": total_count - failed_count,
            "failed_records": failed_count,
            "pass_rate": ((total_count - failed_count) / total_count) * 100
        }
        return passed_df, failed_df, result

    def check_uniqueness(self, df: DataFrame, unique_key: str) -> Tuple[DataFrame, Dict]:
        """Checks for duplicate primary keys."""
        total_count = df.count()
        if total_count == 0:
            return df, self._empty_result("UNIQUENESS", [unique_key])

        # Group by the key and count
        duplicates = df.groupBy(unique_key).agg(count("*").alias("cnt")).filter(col("cnt") > 1)
        
        # Join back to get full bad records
        failed_df = df.join(duplicates.select(unique_key), on=unique_key, how="inner") \
                      .withColumn("failure_reason", lit(f"Duplicate {unique_key}"))
        
        # Clean records
        passed_df = df.join(duplicates.select(unique_key), on=unique_key, how="left_anti")
        
        failed_count = failed_df.count()
        
        result = {
            "check_type": "UNIQUENESS",
            "columns": [unique_key],
            "total_records": total_count,
            "passed_records": total_count - failed_count,
            "failed_records": failed_count,
            "pass_rate": ((total_count - failed_count) / total_count) * 100
        }
        return passed_df, failed_df, result

    def check_range(self, df: DataFrame, column: str, min_val: float, max_val: float = None) -> Tuple[DataFrame, Dict]:
        """Checks if numeric/date values fall within an expected range."""
        total_count = df.count()
        
        condition = col(column) >= min_val
        if max_val is not None:
            condition = condition & (col(column) <= max_val)
            
        passed_df = df.filter(condition)
        failed_df = df.filter(~condition | isnull(col(column))) \
                      .withColumn("failure_reason", lit(f"Out of range: {column}"))
                      
        failed_count = failed_df.count()
        
        result = {
            "check_type": "RANGE",
            "columns": [column],
            "total_records": total_count,
            "passed_records": total_count - failed_count,
            "failed_records": failed_count,
            "pass_rate": ((total_count - failed_count) / total_count) * 100 if total_count > 0 else 100.0
        }
        return passed_df, failed_df, result

    def _empty_result(self, check_type: str, columns: List[str]) -> Dict:
        return {
            "check_type": check_type,
            "columns": columns,
            "total_records": 0,
            "passed_records": 0,
            "failed_records": 0,
            "pass_rate": 100.0
        }
