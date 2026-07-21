"""
Banking Data Engineering Platform — Main Ingestion Entry Point
==============================================================
Orchestrates the full data generation pipeline.

This is the single command to run for data generation.
It:
  1. Loads config from config.yaml + .env
  2. Generates all entity datasets in dependency order
  3. Writes outputs to the data lake (local filesystem or S3)
  4. Logs progress and summary statistics
  5. Saves a small sample dataset for testing

Usage:
    python ingestion/ingest.py --mode dev
    python ingestion/ingest.py --mode large
    python ingestion/ingest.py --mode dev --output-format csv
    python ingestion/ingest.py --mode dev --output-format parquet

Run from the project root directory.
"""

import sys
import os
import logging
import argparse
import time
from pathlib import Path

import pandas as pd

# Add project root to path so config and ingestion modules are importable
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import load_config, setup_logging, get_data_path, get_generation_params
from ingestion.generate_branches import generate_branches
from ingestion.generate_merchants import generate_merchants
from ingestion.generate_customers import generate_customers
from ingestion.generate_accounts import generate_accounts
from ingestion.generate_transactions import generate_transactions

logger = logging.getLogger("banking.ingestion.ingest")


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Banking Data Engineering Platform — Data Generator",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["dev", "large"],
        default="dev",
        help="Generation mode: 'dev' (fast/small) or 'large' (full scale)",
    )
    parser.add_argument(
        "--output-format",
        choices=["csv", "parquet"],
        default="csv",
        help="Output format for raw data files",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility",
    )
    parser.add_argument(
        "--no-sample",
        action="store_true",
        help="Skip generating sample data (useful for CI)",
    )
    return parser.parse_args()


def save_dataframe(
    df: pd.DataFrame,
    path: Path,
    filename: str,
    fmt: str = "csv",
) -> Path:
    """
    Save a DataFrame to the specified path and format.

    Args:
        df:       DataFrame to save.
        path:     Directory path.
        filename: File name without extension.
        fmt:      'csv' or 'parquet'.

    Returns:
        Path to the saved file.
    """
    path.mkdir(parents=True, exist_ok=True)
    ext = "csv" if fmt == "csv" else "parquet"
    full_path = path / f"{filename}.{ext}"

    if fmt == "csv":
        df.to_csv(full_path, index=False)
    else:
        df.to_parquet(full_path, index=False, compression="snappy")

    logger.info(f"Saved {len(df):,} records → {full_path}")
    return full_path


def run_ingestion(
    config: dict,
    mode: str,
    output_format: str = "csv",
    seed: int = 42,
    generate_sample: bool = True,
) -> dict:
    """
    Main ingestion orchestration function.

    Generates all datasets in dependency order:
      1. Branches (no dependencies)
      2. Merchants (no dependencies)
      3. Customers (no dependencies)
      4. Accounts (depends on: customers, branches)
      5. Transactions (depends on: accounts, merchants, branches)

    Args:
        config:          Loaded configuration dictionary.
        mode:            'dev' or 'large'
        output_format:   'csv' or 'parquet'
        seed:            Random seed
        generate_sample: Whether to also write small sample files

    Returns:
        Summary dictionary with record counts and file paths.
    """
    params = get_generation_params(config)
    # Allow CLI mode to override config
    if mode in ("dev", "large"):
        params.update(config["data_generation"][mode])
        params["mode"] = mode

    bad_pct = params["bad_data_percentage"]
    summary = {}
    start_total = time.time()

    logger.info(
        f"=== Starting Data Ingestion | mode={mode} | format={output_format} | "
        f"bad_data={bad_pct:.1%} ==="
    )
    logger.info(
        f"Target volumes: customers={params['customers']:,} | "
        f"accounts={params['accounts']:,} | "
        f"transactions={params['transactions']:,}"
    )

    raw_path = get_data_path(config, "raw")

    # ----------------------------------------------------------------
    # 1. BRANCHES
    # ----------------------------------------------------------------
    t0 = time.time()
    branches_df = generate_branches(count=params["branches"], seed=seed)
    branch_path = save_dataframe(branches_df, raw_path / "branches", "branches", output_format)
    elapsed = time.time() - t0
    summary["branches"] = {"records": len(branches_df), "path": str(branch_path), "seconds": elapsed}
    logger.info(f"✓ Branches: {len(branches_df):,} records in {elapsed:.1f}s")

    # ----------------------------------------------------------------
    # 2. MERCHANTS
    # ----------------------------------------------------------------
    t0 = time.time()
    merchants_df = generate_merchants(count=params["merchants"], seed=seed)
    merchant_path = save_dataframe(merchants_df, raw_path / "merchants", "merchants", output_format)
    elapsed = time.time() - t0
    summary["merchants"] = {"records": len(merchants_df), "path": str(merchant_path), "seconds": elapsed}
    logger.info(f"✓ Merchants: {len(merchants_df):,} records in {elapsed:.1f}s")

    # ----------------------------------------------------------------
    # 3. CUSTOMERS (streamed in batches)
    # ----------------------------------------------------------------
    t0 = time.time()
    customer_chunks = []
    for batch_df in generate_customers(
        count=params["customers"],
        bad_data_pct=bad_pct,
        seed=seed,
        batch_size=params["batch_size"],
    ):
        customer_chunks.append(batch_df)

    customers_df = pd.concat(customer_chunks, ignore_index=True)
    cust_path = save_dataframe(customers_df, raw_path / "customers", "customers", output_format)
    elapsed = time.time() - t0
    summary["customers"] = {"records": len(customers_df), "path": str(cust_path), "seconds": elapsed}
    logger.info(f"✓ Customers: {len(customers_df):,} records in {elapsed:.1f}s")

    # ----------------------------------------------------------------
    # 4. ACCOUNTS (depends on customers + branches)
    # ----------------------------------------------------------------
    t0 = time.time()
    branch_ids = branches_df["branch_id"].tolist()
    account_chunks = []
    for batch_df in generate_accounts(
        customer_df=customers_df,
        branch_ids=branch_ids,
        target_count=params["accounts"],
        bad_data_pct=bad_pct,
        seed=seed,
        batch_size=params["batch_size"],
    ):
        account_chunks.append(batch_df)

    accounts_df = pd.concat(account_chunks, ignore_index=True)
    acct_path = save_dataframe(accounts_df, raw_path / "accounts", "accounts", output_format)
    elapsed = time.time() - t0
    summary["accounts"] = {"records": len(accounts_df), "path": str(acct_path), "seconds": elapsed}
    logger.info(f"✓ Accounts: {len(accounts_df):,} records in {elapsed:.1f}s")

    # ----------------------------------------------------------------
    # 5. TRANSACTIONS (depends on accounts + merchants + branches)
    #    Written directly from generator → disk to avoid RAM overload
    # ----------------------------------------------------------------
    t0 = time.time()
    account_ids = accounts_df["account_id"].dropna().tolist()
    merchant_ids = merchants_df["merchant_id"].tolist()

    tx_path = raw_path / "transactions"
    tx_path.mkdir(parents=True, exist_ok=True)
    tx_total = 0
    batch_num = 0

    for batch_df in generate_transactions(
        account_ids=account_ids,
        merchant_ids=merchant_ids,
        branch_ids=branch_ids,
        total_count=params["transactions"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        bad_data_pct=bad_pct,
        seed=seed,
        batch_size=params["batch_size"],
    ):
        # Write each batch immediately (no accumulation in RAM)
        batch_file = tx_path / f"transactions_batch_{batch_num:04d}"
        if output_format == "csv":
            batch_df.to_csv(f"{batch_file}.csv", index=False)
        else:
            batch_df.to_parquet(f"{batch_file}.parquet", index=False, compression="snappy")

        tx_total += len(batch_df)
        batch_num += 1

        if batch_num % 5 == 0:
            logger.info(f"  Transactions progress: {tx_total:,} written...")

    elapsed = time.time() - t0
    summary["transactions"] = {
        "records": tx_total,
        "batches": batch_num,
        "path": str(tx_path),
        "seconds": elapsed,
    }
    logger.info(f"✓ Transactions: {tx_total:,} records in {elapsed:.1f}s ({batch_num} batches)")

    # ----------------------------------------------------------------
    # 6. SAMPLE DATA (small files for quick testing)
    # ----------------------------------------------------------------
    if generate_sample:
        sample_path = get_data_path(config, "sample")
        _write_sample_data(
            branches_df, merchants_df, customers_df, accounts_df,
            sample_path, seed
        )

    # ----------------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------------
    total_elapsed = time.time() - start_total
    total_records = sum(v["records"] for v in summary.values())

    logger.info("=" * 60)
    logger.info(f"INGESTION COMPLETE in {total_elapsed:.1f}s")
    logger.info(f"Total records generated: {total_records:,}")
    for dataset, info in summary.items():
        logger.info(f"  {dataset:<14}: {info['records']:>10,} records | {info['seconds']:.1f}s")
    logger.info("=" * 60)

    return summary


def _write_sample_data(
    branches_df: pd.DataFrame,
    merchants_df: pd.DataFrame,
    customers_df: pd.DataFrame,
    accounts_df: pd.DataFrame,
    sample_path: Path,
    seed: int,
) -> None:
    """Write small sample CSVs for testing and GitHub."""
    n_sample = {"branches": 20, "merchants": 50, "customers": 100, "accounts": 200}

    for name, df, n in [
        ("branches", branches_df, n_sample["branches"]),
        ("merchants", merchants_df, n_sample["merchants"]),
        ("customers", customers_df, n_sample["customers"]),
        ("accounts", accounts_df, n_sample["accounts"]),
    ]:
        sample = df.sample(min(n, len(df)), random_state=seed)
        out = sample_path / f"sample_{name}.csv"
        sample.to_csv(out, index=False)
        logger.info(f"Sample written: {out}")


def main() -> None:
    """CLI entry point."""
    # Setup logging first
    setup_logging()

    args = parse_args()

    # Override DATA_GENERATION_MODE from CLI arg
    os.environ["DATA_GENERATION_MODE"] = args.mode

    config = load_config()

    logger.info(f"Banking Data Engineering Platform — Ingestion")
    logger.info(f"Mode: {args.mode} | Format: {args.output_format} | Seed: {args.seed}")

    try:
        summary = run_ingestion(
            config=config,
            mode=args.mode,
            output_format=args.output_format,
            seed=args.seed,
            generate_sample=not args.no_sample,
        )
        logger.info("Ingestion finished successfully.")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
