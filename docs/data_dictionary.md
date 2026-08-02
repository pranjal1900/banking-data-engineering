# Banking Data Engineering — Data Dictionary

## 1. Staging Schema (`staging.*`)
The raw landing zone for incoming CSV/JSON data. All columns are stored as `VARCHAR` to prevent type-casting errors from halting the ETL pipeline.

| Table | Description |
|-------|-------------|
| `staging.transactions` | Raw transaction records containing amount, merchant, and timestamp. |
| `staging.accounts` | Raw account balances and statuses. |
| `staging.customers` | Raw customer profiles. |

## 2. Warehouse Schema (`warehouse.*`)
The curated dimensional model (Star Schema) used for analytical querying.

### `dim_customer` (SCD Type 2)
| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | VARCHAR | Primary Key |
| `age` | INT | Customer age |
| `income` | DECIMAL | Annual income |
| `is_active` | BOOLEAN | SCD2: Is this the current record? |

### `fact_transactions`
| Column | Type | Description |
|--------|------|-------------|
| `transaction_id` | VARCHAR | Primary Key |
| `account_id` | VARCHAR | FK to `dim_account` |
| `amount` | DECIMAL | Transaction amount |
| `is_fraudulent`| BOOLEAN | Flagged by fraud engine |

## 3. Quality Schema (`quality.*`)
Metadata for pipeline observability.

| Table | Description |
|-------|-------------|
| `quality_results` | Aggregated pass/fail rates for every check (e.g. Completeness, Uniqueness). |
| `rejected_records` | The Dead Letter Queue (DLQ). Bad records are quarantined here with a `failure_reason`. |
