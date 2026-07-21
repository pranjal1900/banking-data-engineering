"""
Banking Data Engineering Platform — Unit Tests for Ingestion
============================================================
Tests the data generator modules directly.

Run with:
    venv\\Scripts\\pytest tests/unit/test_generators.py -v

These tests verify:
  - Generators produce the correct number of records
  - Required fields exist and have valid types
  - Bad data injection works correctly
  - ID uniqueness constraints
  - Value ranges (age >= 18, amount > 0 for valid records)
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.generate_branches import generate_branches
from ingestion.generate_merchants import generate_merchants
from ingestion.generate_customers import generate_customers
from ingestion.generate_accounts import generate_accounts
from ingestion.generate_transactions import generate_transactions


# ============================================================
# BRANCH TESTS
# ============================================================

class TestBranchGenerator:
    def test_generates_correct_count(self):
        df = generate_branches(count=50, seed=1)
        assert len(df) == 50

    def test_required_fields_exist(self):
        df = generate_branches(count=10, seed=1)
        required = ["branch_id", "branch_name", "city", "state", "branch_type"]
        for col in required:
            assert col in df.columns, f"Missing column: {col}"

    def test_no_null_ids(self):
        df = generate_branches(count=50, seed=1)
        assert df["branch_id"].isna().sum() == 0

    def test_unique_branch_ids(self):
        df = generate_branches(count=100, seed=1)
        assert df["branch_id"].nunique() == len(df)

    def test_valid_branch_types(self):
        valid_types = {"Urban", "Semi-Urban", "Rural", "Metro", "Corporate"}
        df = generate_branches(count=100, seed=1)
        assert set(df["branch_type"].unique()).issubset(valid_types)


# ============================================================
# MERCHANT TESTS
# ============================================================

class TestMerchantGenerator:
    def test_generates_correct_count(self):
        df = generate_merchants(count=100, seed=1)
        assert len(df) == 100

    def test_risk_categories_valid(self):
        valid_risks = {"LOW", "MEDIUM", "HIGH"}
        df = generate_merchants(count=200, seed=1)
        assert set(df["risk_category"].unique()).issubset(valid_risks)

    def test_no_null_merchant_ids(self):
        df = generate_merchants(count=50, seed=1)
        assert df["merchant_id"].isna().sum() == 0

    def test_unique_merchant_ids(self):
        df = generate_merchants(count=200, seed=1)
        assert df["merchant_id"].nunique() == len(df)

    def test_all_categories_present(self):
        # With enough merchants, all categories should appear
        df = generate_merchants(count=500, seed=1)
        assert df["merchant_category"].nunique() >= 8


# ============================================================
# CUSTOMER TESTS
# ============================================================

class TestCustomerGenerator:
    def _collect_all(self, gen) -> pd.DataFrame:
        """Collect all batches from a generator."""
        return pd.concat(list(gen), ignore_index=True)

    def test_generates_approximately_correct_count(self):
        df = self._collect_all(generate_customers(count=500, bad_data_pct=0.0, seed=1))
        assert len(df) == 500

    def test_required_fields_exist(self):
        df = self._collect_all(generate_customers(count=100, bad_data_pct=0.0, seed=1))
        required = ["customer_id", "name", "age", "gender", "city", "state",
                    "occupation", "income", "account_open_date", "customer_segment"]
        for col in required:
            assert col in df.columns

    def test_valid_ages_in_good_data(self):
        # With 0% bad data, all ages should be >= 18
        df = self._collect_all(generate_customers(count=200, bad_data_pct=0.0, seed=1))
        assert (df["age"] >= 18).all(), "Found underage customer in clean data"

    def test_bad_data_injected(self):
        # With 20% bad data, we should see some nulls or violations
        df = self._collect_all(generate_customers(count=500, bad_data_pct=0.20, seed=1))
        # At least some records should have null IDs or null names or underage
        has_null_id = df["customer_id"].isna().any()
        has_null_name = df["name"].isna().any()
        has_underage = (df["age"] < 18).any()
        assert has_null_id or has_null_name or has_underage, \
            "Expected bad data but none found"

    def test_valid_segments(self):
        valid_segments = {"Regular", "Premium", "High Net Worth", "Student", "Senior"}
        df = self._collect_all(generate_customers(count=300, bad_data_pct=0.0, seed=1))
        assert set(df["customer_segment"].unique()).issubset(valid_segments)

    def test_batch_generator_works(self):
        # Verify generator yields multiple batches for large count
        batches = list(generate_customers(count=600, bad_data_pct=0.0, seed=1, batch_size=200))
        assert len(batches) >= 3
        total = sum(len(b) for b in batches)
        assert total == 600


# ============================================================
# ACCOUNT TESTS
# ============================================================

class TestAccountGenerator:
    def _setup(self):
        branches = generate_branches(count=20, seed=1)
        customers_gen = generate_customers(count=200, bad_data_pct=0.0, seed=1)
        customers = pd.concat(list(customers_gen), ignore_index=True)
        return customers, branches["branch_id"].tolist()

    def _collect_all(self, gen) -> pd.DataFrame:
        return pd.concat(list(gen), ignore_index=True)

    def test_generates_accounts(self):
        customers, branch_ids = self._setup()
        df = self._collect_all(
            generate_accounts(customers, branch_ids, target_count=300, bad_data_pct=0.0, seed=1)
        )
        assert len(df) > 0

    def test_required_fields_exist(self):
        customers, branch_ids = self._setup()
        df = self._collect_all(
            generate_accounts(customers, branch_ids, target_count=100, bad_data_pct=0.0, seed=1)
        )
        required = ["account_id", "customer_id", "account_type", "branch_id",
                    "balance", "account_status"]
        for col in required:
            assert col in df.columns

    def test_valid_account_types(self):
        valid_types = {"Savings", "Current", "Salary"}
        customers, branch_ids = self._setup()
        df = self._collect_all(
            generate_accounts(customers, branch_ids, target_count=200, bad_data_pct=0.0, seed=1)
        )
        clean = df[df["account_type"].notna()]
        assert set(clean["account_type"].unique()).issubset(valid_types)

    def test_branch_ids_valid(self):
        customers, branch_ids = self._setup()
        df = self._collect_all(
            generate_accounts(customers, branch_ids, target_count=100, bad_data_pct=0.0, seed=1)
        )
        clean = df[df["branch_id"].notna()]
        assert clean["branch_id"].isin(branch_ids).all()

    def test_bad_data_injected(self):
        customers, branch_ids = self._setup()
        df = self._collect_all(
            generate_accounts(customers, branch_ids, target_count=500, bad_data_pct=0.20, seed=1)
        )
        has_null = df["account_id"].isna().any() or df["customer_id"].isna().any()
        has_negative = (df["balance"] < 0).any()
        has_invalid_status = df["account_status"].isin(["UNKNOWN"]).any()
        assert has_null or has_negative or has_invalid_status


# ============================================================
# TRANSACTION TESTS
# ============================================================

class TestTransactionGenerator:
    def _setup(self):
        branches = generate_branches(count=10, seed=1)
        merchants = generate_merchants(count=50, seed=1)
        customers_gen = generate_customers(count=100, bad_data_pct=0.0, seed=1)
        customers = pd.concat(list(customers_gen), ignore_index=True)
        accounts_gen = generate_accounts(
            customers, branches["branch_id"].tolist(),
            target_count=150, bad_data_pct=0.0, seed=1
        )
        accounts = pd.concat(list(accounts_gen), ignore_index=True)
        return (
            accounts["account_id"].dropna().tolist(),
            merchants["merchant_id"].tolist(),
            branches["branch_id"].tolist(),
        )

    def _collect_all(self, gen) -> pd.DataFrame:
        return pd.concat(list(gen), ignore_index=True)

    def test_generates_correct_count(self):
        acc, mer, brn = self._setup()
        df = self._collect_all(
            generate_transactions(acc, mer, brn, total_count=500, bad_data_pct=0.0,
                                  fraud_seed_pct=0.0, seed=1)
        )
        assert len(df) == 500

    def test_required_fields_exist(self):
        acc, mer, brn = self._setup()
        df = self._collect_all(
            generate_transactions(acc, mer, brn, total_count=100, bad_data_pct=0.0,
                                  fraud_seed_pct=0.0, seed=1)
        )
        required = ["transaction_id", "account_id", "transaction_type",
                    "amount", "timestamp", "merchant_id", "status", "channel"]
        for col in required:
            assert col in df.columns

    def test_valid_amounts_in_clean_data(self):
        acc, mer, brn = self._setup()
        df = self._collect_all(
            generate_transactions(acc, mer, brn, total_count=300, bad_data_pct=0.0,
                                  fraud_seed_pct=0.0, seed=1)
        )
        assert (df["amount"] > 0).all(), "Negative amounts found in clean data"

    def test_fraud_patterns_seeded(self):
        acc, mer, brn = self._setup()
        df = self._collect_all(
            generate_transactions(acc, mer, brn, total_count=200, bad_data_pct=0.0,
                                  fraud_seed_pct=0.10, seed=1)
        )
        fraud_rows = df[df["transaction_id"].str.startswith("FRAUD-", na=False)]
        assert len(fraud_rows) > 0, "No fraud-seeded transactions found"

    def test_bad_data_injected(self):
        acc, mer, brn = self._setup()
        df = self._collect_all(
            generate_transactions(acc, mer, brn, total_count=500, bad_data_pct=0.20,
                                  fraud_seed_pct=0.0, seed=1)
        )
        has_null_tx = df["transaction_id"].isna().any()
        has_null_acc = df["account_id"].isna().any()
        has_negative = (df["amount"] < 0).any()
        assert has_null_tx or has_null_acc or has_negative
