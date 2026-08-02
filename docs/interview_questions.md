# Data Engineering Interview Questions & Context

This document provides answers to common Data Engineering interview questions, contextualized specifically to this project.

## 1. "Walk me through a complex data pipeline you've built."
**Your Answer:**
"I built an end-to-end banking pipeline. The source was a Python application generating millions of mocked transactions, accounts, and customers in CSV format. I built an Airflow DAG to orchestrate a daily batch process. The raw files were picked up by PySpark jobs. First, I ran the data through a custom Data Quality framework I built which quarantined bad records into a PostgreSQL Dead Letter Queue. Then, I applied data cleansing and enrichment transformations, and evaluated a heuristic Fraud Analytics rules engine on the transactions. Finally, the enriched data was written to a dimensional Star Schema in PostgreSQL, optimized for BI dashboards."

## 2. "How did you optimize your PySpark jobs?"
**Your Answer:**
"In my `load_warehouse.py` job, I used **Broadcast Joins**. When joining the massive `transactions` fact table with smaller dimension tables like `branches` and `merchants`, I used PySpark's `broadcast()` function. This sends a copy of the small table to every worker node, completely eliminating network shuffling (which is the most expensive operation in Spark). I also cached intermediate DataFrames (like the clean transactions) because they were accessed multiple times by both the Enrichment engine and the Fraud engine."

## 3. "How did you handle Data Quality?"
**Your Answer:**
"Instead of letting bad data crash my Airflow DAG at 3 AM, I implemented a quarantine pattern. My Data Quality PySpark module evaluated rules like Completeness (checking for nulls) and Ranges (e.g., negative transaction amounts). The engine splits the DataFrame: good records continue down the pipeline, while bad records are written to a `quality.rejected_records` table in PostgreSQL along with the exact `failure_reason`. This allows Data Analysts to investigate the failures without stopping the daily load."

## 4. "How did you make your pipeline incremental?"
**Your Answer:**
"I implemented a Watermarking system. At the start of the PySpark job, it queries a `quality.pipeline_watermarks` table to find the `last_processed` timestamp for a given dataset (e.g., Transactions). Spark then filters the incoming raw files and only processes records with a timestamp greater than the watermark. After the job succeeds, the watermark table is updated. This reduced our compute costs by avoiding full-table scans of the data lake."

## 5. "Why use a Star Schema instead of a highly normalized database (3NF)?"
**Your Answer:**
"A highly normalized database is great for software applications (OLTP) because it avoids data duplication and ensures fast single-row writes. However, it requires many JOINs to read data. For analytics (OLAP), read speed is paramount. I designed a Star Schema with denormalized Dimension tables surrounding a central Fact table. This significantly reduced the number of JOINs required by BI tools like PowerBI, making dashboards much more responsive when aggregating millions of rows."
