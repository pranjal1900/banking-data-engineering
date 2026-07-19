"""
Banking Data Engineering Platform — Customer Generator
=======================================================
Generates realistic synthetic Indian banking customer data.

Design decisions:
  - Uses Faker with en_IN locale for Indian names, cities
  - Customer segments drive income distributions (realistic co-variation)
  - Age ranges differ by segment (Students are younger, Seniors are older)
  - account_open_date is staggered to simulate real customer onboarding
  - No real PII — all data is synthetic

Interview talking point:
  "Customers are a slowly-changing dimension (SCD). In a real bank,
  a customer's address or segment can change over time. For this
  project, we use SCD Type 1 (overwrite) for simplicity, but I'm
  aware SCD Type 2 (add row with effective dates) would be needed
  for historical tracking in production."
"""

import random
import logging
from datetime import date, timedelta
from typing import Iterator

import numpy as np
import pandas as pd
from faker import Faker

logger = logging.getLogger("banking.ingestion.customers")
fake = Faker("en_IN")

# ---- Customer segments with realistic distributions ----
SEGMENTS = ["Regular", "Premium", "High Net Worth", "Student", "Senior"]
SEGMENT_WEIGHTS = [0.50, 0.20, 0.05, 0.15, 0.10]

# Income ranges (annual, INR) by segment
INCOME_RANGES = {
    "Regular":        (180000, 800000),
    "Premium":        (800000, 2500000),
    "High Net Worth": (2500000, 15000000),
    "Student":        (0, 150000),       # Part-time / stipend
    "Senior":         (120000, 600000),  # Pension / retirement
}

# Age ranges by segment
AGE_RANGES = {
    "Regular":        (25, 55),
    "Premium":        (30, 55),
    "High Net Worth": (35, 65),
    "Student":        (18, 26),
    "Senior":         (60, 80),
}

GENDERS = ["Male", "Female", "Other"]
GENDER_WEIGHTS = [0.52, 0.46, 0.02]

OCCUPATIONS = [
    "Software Engineer", "Doctor", "Teacher", "Business Owner",
    "Government Employee", "Bank Employee", "Lawyer", "Chartered Accountant",
    "Sales Executive", "Engineer", "Nurse", "Entrepreneur",
    "Retired", "Student", "Homemaker", "Freelancer",
]

INDIAN_CITIES_STATES = [
    ("Mumbai", "Maharashtra"), ("Pune", "Maharashtra"), ("Nagpur", "Maharashtra"),
    ("Delhi", "Delhi"), ("Noida", "Uttar Pradesh"), ("Gurgaon", "Haryana"),
    ("Bengaluru", "Karnataka"), ("Mysuru", "Karnataka"),
    ("Chennai", "Tamil Nadu"), ("Coimbatore", "Tamil Nadu"),
    ("Hyderabad", "Telangana"), ("Visakhapatnam", "Andhra Pradesh"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Surat", "Gujarat"),
    ("Jaipur", "Rajasthan"), ("Lucknow", "Uttar Pradesh"), ("Kanpur", "Uttar Pradesh"),
    ("Bhopal", "Madhya Pradesh"), ("Patna", "Bihar"), ("Kochi", "Kerala"),
    ("Chandigarh", "Punjab"), ("Dehradun", "Uttarakhand"), ("Guwahati", "Assam"),
]

# Reference start for account_open_date
ONBOARDING_START = date(2015, 1, 1)
ONBOARDING_END = date(2024, 6, 30)


def generate_customers(
    count: int = 10000,
    bad_data_pct: float = 0.05,
    seed: int = 42,
    batch_size: int = 50000,
) -> Iterator[pd.DataFrame]:
    """
    Generate synthetic customer records in batches (generator pattern).

    Why a generator?
      For large-scale mode (100K+ records), loading everything into RAM
      at once would be wasteful. A generator yields DataFrames in chunks,
      allowing the caller to write each chunk to disk / S3 before
      the next chunk is loaded. This is the same pattern used in
      production ETL pipelines.

    Args:
        count:        Total customers to generate.
        bad_data_pct: Fraction of records with intentional defects.
        seed:         Random seed for reproducibility.
        batch_size:   Records per yielded DataFrame.

    Yields:
        pandas DataFrame of up to batch_size customer records.

    Bad data injected (configurable %):
      - NULL customer_id
      - NULL name
      - Age below 18 (invalid)
      - Duplicate customer_id
    """
    random.seed(seed)
    np.random.seed(seed)
    fake.seed_instance(seed)

    logger.info(f"Starting customer generation: count={count}, bad_data_pct={bad_data_pct:.1%}")

    bad_count = int(count * bad_data_pct)
    good_count = count - bad_count
    bad_indices = set(random.sample(range(count), min(bad_count, count)))

    # Pre-generate IDs to allow intentional duplicates in bad records
    customer_ids = _generate_ids("CUST", count, seed)

    batch_records = []

    for i in range(count):
        is_bad = i in bad_indices
        record = _build_customer_record(i, customer_ids, is_bad, seed)
        batch_records.append(record)

        if len(batch_records) >= batch_size:
            yield pd.DataFrame(batch_records)
            batch_records = []

    # Yield remaining records
    if batch_records:
        yield pd.DataFrame(batch_records)

    logger.info(f"Customer generation complete: good={good_count}, bad_injected={bad_count}")


def _build_customer_record(index: int, all_ids: list, is_bad: bool, seed: int) -> dict:
    """Build a single customer record, with optional bad data injection."""
    segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
    age_min, age_max = AGE_RANGES[segment]
    income_min, income_max = INCOME_RANGES[segment]
    city, state = random.choice(INDIAN_CITIES_STATES)

    age = random.randint(age_min, age_max)
    income = int(np.random.uniform(income_min, income_max))

    # Account open date: more recent for younger segments
    open_date = _random_date(ONBOARDING_START, ONBOARDING_END)

    record = {
        "customer_id": all_ids[index],
        "name": fake.name(),
        "age": age,
        "gender": random.choices(GENDERS, weights=GENDER_WEIGHTS, k=1)[0],
        "city": city,
        "state": state,
        "occupation": random.choice(OCCUPATIONS),
        "income": income,
        "account_open_date": open_date.isoformat(),
        "customer_segment": segment,
        "ingestion_timestamp": pd.Timestamp.now().isoformat(),
    }

    # Inject bad data (intentional defects for quality framework to catch)
    if is_bad:
        defect_type = random.choice(["null_id", "null_name", "underage", "duplicate_id"])
        if defect_type == "null_id":
            record["customer_id"] = None
        elif defect_type == "null_name":
            record["name"] = None
        elif defect_type == "underage":
            record["age"] = random.randint(5, 17)   # Below minimum 18
        elif defect_type == "duplicate_id":
            # Use an ID from the first 10% of records → guaranteed duplicate
            dup_idx = random.randint(0, max(1, int(len(all_ids) * 0.1)))
            record["customer_id"] = all_ids[dup_idx]

    return record


def _generate_ids(prefix: str, count: int, seed: int) -> list:
    """Generate a list of unique IDs."""
    random.seed(seed)
    return [f"{prefix}-{i + 1:07d}" for i in range(count)]


def _random_date(start: date, end: date) -> date:
    """Return a random date between start and end."""
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))
