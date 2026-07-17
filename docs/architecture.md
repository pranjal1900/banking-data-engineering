# Banking Data Engineering Platform — Architecture

## Overview

This document describes the technical architecture of the platform in detail.

---

## System Design Principles

| Principle | Implementation |
|-----------|---------------|
| **Separation of Concerns** | Each module has one responsibility (generate, validate, transform, load) |
| **Idempotency** | Running the same pipeline twice produces the same result |
| **Incremental Processing** | Only new records are processed on each run |
| **Observability** | Every pipeline run is logged and stored in `quality.pipeline_runs` |
| **Testability** | All modules are independently testable |
| **Portability** | Runs locally (Docker) or on cloud (AWS) without code changes |

---

## Data Flow

```mermaid
flowchart TD
    A[Synthetic Data Generator] --> B[Raw Data - CSV/JSON]
    B --> C{Storage Layer}
    C -->|local| D[Local Filesystem]
    C -->|s3| E[AWS S3]
    D --> F[Schema Validation]
    E --> F
    F --> G[Data Quality Checks]
    G -->|valid| H[PySpark Transformations]
    G -->|invalid| I[Rejected Records Store]
    H --> J[Enrichment + Fraud Rules + Aggregations]
    J --> K[PostgreSQL Data Warehouse]
    K --> L[Analytics Tables]
    L --> M[Power BI Dashboard]
    
    N[Apache Airflow] -.->|orchestrates| A
    N -.->|orchestrates| F
    N -.->|orchestrates| H
    N -.->|orchestrates| K
```

---

## Data Lake Layers

### Why a Data Lake?

A data lake stores data in its **native format** before transformation.
This is important because:
- Raw data is preserved for debugging and re-processing
- Schema changes can be handled without data loss
- Multiple consumers can process the same raw data differently

### Layer Structure

```
data/
├── raw/           ← Original data, never modified
│   ├── customers/
│   ├── accounts/
│   ├── transactions/  (partitioned by date in production)
│   ├── merchants/
│   └── branches/
│
├── processed/     ← Spark-cleaned data in Parquet
│   ├── customers/
│   ├── accounts/
│   └── transactions/
│
├── curated/       ← Dimensional data ready for warehouse
│   ├── fact_transactions/
│   ├── dim_customer/
│   ├── dim_account/
│   ├── dim_merchant/
│   ├── dim_branch/
│   └── dim_date/
│
└── rejected/      ← Failed quality checks (never silently deleted)
    ├── customers/
    ├── accounts/
    └── transactions/
```

### Why Partitioning?

In production, transactions are partitioned by date:
```
transactions/year=2024/month=08/day=12/part-0000.parquet
```

**Why this matters:**
- A query for "August 12 transactions" reads **only** August 12 partitions
- Without partitioning, the query scans ALL transaction files
- At 5M records/day, partitioning reduces read I/O by 99%+
- Spark and AWS Athena both support partition pruning automatically

---

## Star Schema Design

```mermaid
erDiagram
    fact_transactions {
        varchar transaction_id PK
        varchar customer_id FK
        varchar account_id FK
        varchar merchant_id FK
        varchar branch_id FK
        integer date_id FK
        decimal amount
        varchar transaction_type
        varchar status
        varchar payment_method
        varchar channel
        varchar location
        timestamp transaction_timestamp
    }
    
    dim_customer {
        varchar customer_id PK
        varchar name
        integer age
        varchar gender
        varchar city
        varchar state
        varchar occupation
        decimal income
        varchar customer_segment
    }
    
    dim_account {
        varchar account_id PK
        varchar customer_id FK
        varchar account_type
        varchar branch_id FK
        decimal balance
        varchar account_status
    }
    
    dim_merchant {
        varchar merchant_id PK
        varchar merchant_name
        varchar merchant_category
        varchar city
        varchar risk_category
    }
    
    dim_branch {
        varchar branch_id PK
        varchar branch_name
        varchar city
        varchar state
        varchar branch_type
    }
    
    dim_date {
        integer date_id PK
        date date
        integer day
        integer month
        integer quarter
        integer year
        varchar weekday
        boolean is_weekend
    }
    
    fact_transactions }o--|| dim_customer : "belongs to"
    fact_transactions }o--|| dim_account : "uses"
    fact_transactions }o--|| dim_merchant : "at"
    fact_transactions }o--|| dim_branch : "processed by"
    fact_transactions }o--|| dim_date : "on"
    dim_account }o--|| dim_customer : "owned by"
```

### Why Star Schema?

| Benefit | Explanation |
|---------|-------------|
| **Query Performance** | Joins are simple (fact → dimension), no complex multi-joins |
| **BI Tool Compatible** | Power BI, Tableau work natively with star schemas |
| **Denormalized** | Dimension attributes stored on dimension, not fact — reduces joins |
| **Aggregation Friendly** | `GROUP BY dim_customer.segment` is a single join + group |

---

## Incremental Processing Strategy

### Problem

At 5M transactions/day, re-processing the entire history on each run is wasteful.

### Solution: Watermark-Based Incremental

```
Day 1 Run:
  → Process ALL records
  → Store watermark: last_processed = "2024-01-31 23:59:59"

Day 2 Run:
  → Read watermark: "2024-01-31 23:59:59"
  → SELECT * FROM raw WHERE ingestion_timestamp > watermark
  → Process only NEW records (e.g., 150,000)
  → Update watermark: last_processed = "2024-02-01 23:59:59"
```

### Late-Arriving Data

Records arriving late (e.g., offline UPI transactions syncing 2 days later)
are handled by a `late_arriving_days` buffer:

```
effective_watermark = stored_watermark - late_arriving_days
```

This re-processes the last N days to catch late records.
The idempotency guarantee (upsert/MERGE) ensures no duplicates.
