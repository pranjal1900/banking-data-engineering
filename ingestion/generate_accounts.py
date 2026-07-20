"""
Banking Data Engineering Platform — Account Generator
======================================================
Generates synthetic bank account data linked to customers and branches.

Key design decisions:
  - Each customer has 1–3 accounts (realistic: most people have savings + 1 more)
  - Premium/HNW customers have higher balances (correlated with segment)
  - Account status is mostly ACTIVE with small % BLOCKED/CLOSED (realistic)
  - Every account references a valid customer_id and branch_id

Interview talking point:
  "Account-to-customer is a one-to-many relationship. In the star schema,
  accounts are modeled as a dimension (dim_account) linked to fact_transactions.
  Customers are a separate dimension so we can slice transactions by both
  account-level and customer-level attributes independently."
"""

import random
import logging
from datetime import date, timedelta
from typing import Iterator, List

import numpy as np
import pandas as pd

logger = logging.getLogger("banking.ingestion.accounts")

ACCOUNT_TYPES = ["Savings", "Current", "Salary"]
ACCOUNT_TYPE_WEIGHTS = [0.60, 0.25, 0.15]

ACCOUNT_STATUSES = ["ACTIVE", "BLOCKED", "CLOSED"]
STATUS_WEIGHTS = [0.88, 0.07, 0.05]

# Opening balance ranges (INR) by account type
BALANCE_RANGES = {
    "Savings": (1000, 500000),
    "Current": (10000, 5000000),
    "Salary":  (0, 200000),
}

# Segment multipliers on balance (HNW customers have higher balances)
SEGMENT_BALANCE_MULTIPLIER = {
    "Regular":        1.0,
    "Premium":        3.0,
    "High Net Worth": 15.0,
    "Student":        0.3,
    "Senior":         1.5,
}


def generate_accounts(
    customer_df: pd.DataFrame,
    branch_ids: List[str],
    target_count: int = 20000,
    bad_data_pct: float = 0.05,
    seed: int = 42,
    batch_size: int = 50000,
) -> Iterator[pd.DataFrame]:
    """
    Generate synthetic account records linked to customers and branches.

    Args:
        customer_df:  DataFrame of valid customers (provides customer_id, segment).
        branch_ids:   List of valid branch IDs from the branches dataset.
        target_count: Target total number of accounts.
        bad_data_pct: Fraction of records with intentional defects.
        seed:         Random seed for reproducibility.
        batch_size:   Records per yielded DataFrame.

    Yields:
        pandas DataFrame of up to batch_size account records.

    Bad data injected:
      - NULL account_id
      - NULL customer_id
      - Invalid (non-existent) customer_id
      - Negative balance
      - Invalid account status
    """
    random.seed(seed)
    np.random.seed(seed)

    logger.info(
        f"Generating ~{target_count} accounts for {len(customer_df)} customers..."
    )

    valid_customers = customer_df[
        customer_df["customer_id"].notna()
    ][["customer_id", "customer_segment"]].to_dict("records")

    # Distribute accounts across customers (1–3 per customer)
    # This ensures realistic customer→account cardinality
    account_assignments = _assign_accounts_to_customers(
        valid_customers, target_count, seed
    )

    bad_count = int(target_count * bad_data_pct)
    bad_indices = set(random.sample(range(target_count), min(bad_count, target_count)))

    batch_records = []
    account_counter = 0
    used_ids = set()

    for customer_info, num_accounts in account_assignments:
        for _ in range(num_accounts):
            if account_counter >= target_count:
                break

            account_id = _generate_account_id(account_counter, used_ids)
            used_ids.add(account_id)
            is_bad = account_counter in bad_indices

            record = _build_account_record(
                account_id,
                customer_info,
                branch_ids,
                is_bad,
                used_ids,
            )
            batch_records.append(record)
            account_counter += 1

            if len(batch_records) >= batch_size:
                yield pd.DataFrame(batch_records)
                batch_records = []

    if batch_records:
        yield pd.DataFrame(batch_records)

    logger.info(
        f"Account generation complete: total={account_counter}, "
        f"bad_injected={bad_count}"
    )


def _assign_accounts_to_customers(
    customers: list, target_count: int, seed: int
) -> List[tuple]:
    """
    Assign a number of accounts (1–3) to each customer.

    Returns a list of (customer_info, num_accounts) tuples.
    Total accounts will approximate target_count.
    """
    random.seed(seed)
    assignments = []
    total = 0

    for customer in customers:
        if total >= target_count:
            break
        # Most customers have 1 account; some have 2 or 3
        num = random.choices([1, 2, 3], weights=[0.65, 0.27, 0.08], k=1)[0]
        assignments.append((customer, min(num, target_count - total)))
        total += num

    return assignments


def _build_account_record(
    account_id: str,
    customer_info: dict,
    branch_ids: List[str],
    is_bad: bool,
    used_ids: set,
) -> dict:
    """Build a single account record with optional bad data."""
    account_type = random.choices(ACCOUNT_TYPES, weights=ACCOUNT_TYPE_WEIGHTS, k=1)[0]
    status = random.choices(ACCOUNT_STATUSES, weights=STATUS_WEIGHTS, k=1)[0]

    segment = customer_info.get("customer_segment", "Regular")
    multiplier = SEGMENT_BALANCE_MULTIPLIER.get(segment, 1.0)
    bal_min, bal_max = BALANCE_RANGES[account_type]
    balance = round(random.uniform(bal_min * multiplier, bal_max * multiplier), 2)

    open_date = _random_date(date(2015, 1, 1), date(2024, 6, 30))

    record = {
        "account_id":     account_id,
        "customer_id":    customer_info["customer_id"],
        "account_type":   account_type,
        "branch_id":      random.choice(branch_ids),
        "balance":        balance,
        "account_status": status,
        "created_at":     open_date.isoformat(),
        "ingestion_timestamp": pd.Timestamp.now().isoformat(),
    }

    if is_bad:
        defect = random.choice([
            "null_account_id", "null_customer_id",
            "invalid_customer_id", "negative_balance", "invalid_status"
        ])
        if defect == "null_account_id":
            record["account_id"] = None
        elif defect == "null_customer_id":
            record["customer_id"] = None
        elif defect == "invalid_customer_id":
            record["customer_id"] = "CUST-INVALID-999999"  # Non-existent
        elif defect == "negative_balance":
            record["balance"] = round(-random.uniform(100, 50000), 2)
        elif defect == "invalid_status":
            record["account_status"] = "UNKNOWN"   # Not in allowed list

    return record


def _generate_account_id(index: int, used_ids: set) -> str:
    """Generate a unique account ID like ACC-0000001."""
    candidate = f"ACC-{index + 1:07d}"
    while candidate in used_ids:
        candidate = f"ACC-{random.randint(1000000, 9999999)}"
    return candidate


def _random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))
