# 🏦 Banking Transaction Data Engineering & Fraud Analytics Platform

> A production-style, end-to-end banking data platform built to demonstrate practical Data Engineering skills for a Data Engineer role at IDFC FIRST Bank.

![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9-red)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)

---

## 📌 Project Overview

This project simulates a banking organization's **data platform** that:

1. Receives synthetic customer, account, merchant, branch, and transaction data
2. Processes it through ETL pipelines with schema validation and data quality checks
3. Stores it in a **Data Lake** (local filesystem / AWS S3)
4. Transforms it using **PySpark**
5. Applies **rule-based fraud detection** using SQL window functions
6. Loads clean data into a **PostgreSQL Data Warehouse** (star schema)
7. Performs **data reconciliation** (source vs. target count/amount checks)
8. Generates **analytics tables** for business intelligence
9. Orchestrates everything with **Apache Airflow**

---

## 🎯 Business Problem

Banks process **millions of transactions daily**. The core Data Engineering challenges are:

- **Ingesting** high-volume transaction data reliably
- **Validating** data quality before it enters the warehouse
- **Detecting fraud** using rule-based analytics on transaction patterns
- **Reconciling** source systems with the data warehouse to ensure integrity
- **Processing incrementally** — not reprocessing the full history every run
- **Auditing** every pipeline run for compliance and debugging

---

## 🏗️ Architecture

```
                     BANKING DATA SOURCES
                              │
                              ▼
                  ┌───────────────────────┐
                  │ Python Data Generator │
                  │   (Faker + NumPy)     │
                  └───────────┬───────────┘
                              │
                              ▼
                     ┌────────────────┐
                     │   RAW DATA     │
                     │  CSV / JSON    │
                     └───────┬────────┘
                             │
                             ▼
                     ┌────────────────┐
                     │  Data Lake     │
                     │  Local / S3    │
                     └───────┬────────┘
                             │
                    ┌────────┴─────────┐
                    ▼                  ▼
            Schema Validation    Data Quality
                    │                  │
                    └────────┬─────────┘
                             │
                             ▼
                     ┌────────────────┐
                     │    PySpark     │
                     │ Transformations│
                     └───┬───┬───┬───┘
                         │   │   │
                      Enrich Fraud Agg
                         │   │   │
                         └───┴───┘
                             │
                             ▼
                     ┌────────────────┐
                     │   PostgreSQL   │
                     │ Data Warehouse │
                     │  (Star Schema) │
                     └───────┬────────┘
                             │
                             ▼
                     ┌────────────────┐
                     │  Reconciliation│
                     │  + Analytics   │
                     └───────┬────────┘
                             │
                             ▼
                       Power BI / SQL

              Apache Airflow orchestrates ALL stages
```

---

## 🛠️ Technology Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Data Generation | Python, Faker, NumPy | Realistic synthetic banking data |
| Data Processing | PySpark 3.5 | Scalable, distributed transformations |
| Orchestration | Apache Airflow 2.9 | DAG-based pipeline scheduling |
| Database | PostgreSQL 15 | Reliable OLAP warehouse for analytics |
| Data Lake | Local FS / AWS S3 | Raw/processed/curated data layers |
| Containerization | Docker + Compose | Reproducible dev environment |
| Storage Format | CSV (raw), Parquet (processed) | Columnar format for analytics |
| Analytics | Power BI | Business dashboards |
| Testing | pytest | Unit + integration + data quality |
| Version Control | Git + GitHub | Professional workflow |

---

## 📁 Folder Structure

```
banking-data-engineering/
│
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── .gitignore
├── .env.example                 # Environment variable template
├── docker-compose.yml           # Infrastructure definition
│
├── config/
│   ├── config.yaml              # Main configuration
│   └── logging.yaml             # Logging configuration
│
├── data/
│   ├── raw/                     # Raw CSV from generator
│   ├── processed/               # Spark-processed Parquet
│   ├── curated/                 # Final dimensional data
│   ├── rejected/                # Failed data quality records
│   └── sample/                  # Small samples for testing
│
├── ingestion/
│   ├── generate_customers.py    # Customer data generator
│   ├── generate_accounts.py     # Account data generator
│   ├── generate_transactions.py # Transaction data generator
│   ├── generate_merchants.py    # Merchant data generator
│   ├── generate_branches.py     # Branch data generator
│   └── ingest.py                # Main ingestion entry point
│
├── spark/
│   ├── transformations/         # PySpark transformation jobs
│   ├── quality/                 # Data quality checks
│   ├── fraud/                   # Fraud detection rules
│   ├── incremental/             # Incremental processing logic
│   └── jobs/                    # Spark job entry points
│
├── airflow/
│   ├── dags/                    # Airflow DAG definitions
│   ├── plugins/                 # Custom Airflow operators
│   └── requirements.txt        # Airflow-specific deps
│
├── sql/
│   ├── staging/                 # Staging schema DDL
│   ├── warehouse/               # Warehouse DDL
│   ├── analytics/               # Analytics views/tables
│   └── quality/                 # Quality tracking tables
│
├── warehouse/
│   ├── schema/                  # Full schema DDL scripts
│   └── seeds/                   # Reference/lookup data
│
├── tests/
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── data_quality/            # Data quality tests
│
├── dashboard/
│   ├── README.md                # Power BI setup guide
│   └── queries/                 # SQL queries for BI
│
├── docker/
│   ├── airflow/                 # Airflow Dockerfile
│   ├── spark/                   # Spark Dockerfile
│   └── postgres/                # PostgreSQL init scripts
│
├── scripts/
│   ├── setup.ps1                # Windows setup script
│   ├── setup.sh                 # Linux/Mac setup script
│   ├── run_pipeline.ps1         # Pipeline runner
│   └── cleanup.ps1              # Cleanup script
│
└── docs/
    ├── architecture.md          # Detailed architecture docs
    ├── data_dictionary.md       # Table/column definitions
    ├── pipeline.md              # Pipeline explanation
    ├── data_quality.md          # Quality framework docs
    ├── fraud_detection.md       # Fraud rules explanation
    ├── optimization.md          # Spark optimization notes
    └── interview_questions.md   # Interview prep Q&A
```

---

## ⚡ Quick Start

### Prerequisites

- Docker Desktop (for PostgreSQL + Airflow)
- Java JDK 17 (for PySpark)
- Python 3.11+
- Git

### 1. Clone the repository

```bash
git clone https://github.com/your-username/banking-data-engineering.git
cd banking-data-engineering
```

### 2. Set up environment variables

```bash
cp .env.example .env
# Edit .env with your settings
```

### 3. Start infrastructure

```bash
docker compose up -d
```

### 4. Create virtual environment

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

pip install -r requirements.txt
```

### 5. Generate synthetic banking data

```bash
python ingestion/ingest.py --mode dev
```

### 6. Run the full pipeline

```bash
python scripts/run_pipeline.ps1
```

### 7. Open Airflow UI

Navigate to: http://localhost:8080  
Username: `airflow` | Password: `airflow`

---

## 🗃️ Data Model

### Star Schema

```
            dim_customer
                 │
dim_date ─── fact_transactions ─── dim_account
                 │                      │
           dim_merchant            dim_branch
```

### Data Volumes (Dev Mode)

| Entity | Records |
|--------|---------|
| Customers | 10,000 |
| Accounts | 20,000 |
| Transactions | 100,000 |
| Merchants | 500 |
| Branches | 100 |

### Data Volumes (Large-Scale Mode)

| Entity | Records |
|--------|---------|
| Customers | 100,000+ |
| Accounts | 150,000+ |
| Transactions | 5–10 million |

---

## 🔍 Data Quality

The pipeline validates:

- **Completeness** — Required fields are not NULL
- **Uniqueness** — Transaction IDs are unique
- **Validity** — Statuses and types match allowed values
- **Referential Integrity** — account_id exists in accounts table
- **Range Checks** — amount > 0, age >= 18
- **Timestamp Checks** — No future-dated transactions

Bad records are routed to `data/rejected/` and logged in the `quality` schema.

---

## 🚨 Fraud Detection Rules

| Rule | Description |
|------|-------------|
| HIGH_VELOCITY | 5+ transactions within 10 minutes |
| IMPOSSIBLE_LOCATION | 2 transactions in different cities within 30 minutes |
| LARGE_TRANSACTION | Amount > 10x customer average |
| FAILED_THEN_SUCCESS | 3+ FAILEDs followed by SUCCESS |
| UNUSUAL_FREQUENCY | Daily transaction count > 3x 30-day average |

---

## 📊 Dashboard Pages

1. **Executive Overview** — Total volume, value, success rate
2. **Customer Analytics** — Top customers, segments, spending
3. **Branch Analytics** — Branch performance, city/state breakdown
4. **Fraud Analytics** — Alerts by severity, rule, trend over time

---

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run unit tests only
pytest tests/unit/

# Run data quality tests
pytest tests/data_quality/
```

---

## 📄 Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/architecture.md) | System design deep-dive |
| [Data Dictionary](docs/data_dictionary.md) | All tables and columns |
| [Pipeline](docs/pipeline.md) | ETL flow explanation |
| [Data Quality](docs/data_quality.md) | Quality framework |
| [Fraud Detection](docs/fraud_detection.md) | Rule documentation |
| [Optimization](docs/optimization.md) | Spark tuning notes |
| [Interview Questions](docs/interview_questions.md) | Interview prep |

---

## 🎓 Resume Bullets

**Banking Transaction Data Engineering & Fraud Analytics Platform**
*Python | SQL | PySpark | Airflow | AWS S3 | PostgreSQL | Docker | Power BI*

- Designed and implemented an end-to-end batch ETL pipeline processing synthetic banking transactions using PySpark, Apache Airflow, and PostgreSQL, with a modular architecture separating ingestion, validation, transformation, and loading concerns
- Built a reusable data quality framework detecting NULL violations, duplicate records, referential integrity failures, and invalid statuses — routing rejected records to a quarantine layer with structured failure reasons
- Implemented 5 rule-based fraud detection rules using Spark window functions and SQL analytics, generating prioritized fraud alerts with risk scores and severity classifications (LOW/MEDIUM/HIGH/CRITICAL)
- Designed an incremental processing strategy using watermark-based change detection to process only new transactions on each pipeline run, avoiding full-history reprocessing
- Implemented banking-style source-to-target reconciliation comparing record counts and transaction amounts between the raw data lake and the PostgreSQL warehouse

---

## ⚠️ Disclaimer

This is a **portfolio simulation** inspired by banking data engineering use cases.
It does not replicate IDFC FIRST Bank's internal systems.
All data is fully synthetic — no real customer data is used.

---

*Built for Data Engineer interview preparation — IDFC FIRST Bank*
