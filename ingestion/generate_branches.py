"""
Banking Data Engineering Platform — Branch Generator
=====================================================
Generates realistic Indian bank branch data.

Why branches first?
  Accounts are linked to branches. Generating branches first
  ensures foreign key relationships can be resolved during
  account generation.

Interview talking point:
  "I generate reference data (branches, merchants) before
  transactional data (accounts, transactions) to maintain
  referential integrity in the synthetic dataset — exactly
  as you'd handle dimension tables before fact tables in ETL."
"""

import random
import logging
import pandas as pd
from faker import Faker

logger = logging.getLogger("banking.ingestion.branches")
fake = Faker("en_IN")

# ---- Indian city/state mapping (realistic banking data) ----
INDIAN_LOCATIONS = [
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Nagpur", "Maharashtra"),
    ("Delhi", "Delhi"),
    ("Noida", "Uttar Pradesh"),
    ("Gurgaon", "Haryana"),
    ("Bengaluru", "Karnataka"),
    ("Mysuru", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Coimbatore", "Tamil Nadu"),
    ("Hyderabad", "Telangana"),
    ("Visakhapatnam", "Andhra Pradesh"),
    ("Kolkata", "West Bengal"),
    ("Ahmedabad", "Gujarat"),
    ("Surat", "Gujarat"),
    ("Jaipur", "Rajasthan"),
    ("Jodhpur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
    ("Kanpur", "Uttar Pradesh"),
    ("Bhopal", "Madhya Pradesh"),
    ("Indore", "Madhya Pradesh"),
    ("Patna", "Bihar"),
    ("Chandigarh", "Punjab"),
    ("Ludhiana", "Punjab"),
    ("Kochi", "Kerala"),
    ("Thiruvananthapuram", "Kerala"),
    ("Bhubaneswar", "Odisha"),
    ("Guwahati", "Assam"),
    ("Dehradun", "Uttarakhand"),
    ("Raipur", "Chhattisgarh"),
]

BRANCH_TYPES = ["Urban", "Semi-Urban", "Rural", "Metro", "Corporate"]

# Weight toward Urban/Metro since IDFC FIRST Bank is predominantly urban
BRANCH_TYPE_WEIGHTS = [0.35, 0.25, 0.15, 0.20, 0.05]


def generate_branches(count: int = 100, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic bank branch data.

    Args:
        count: Number of branches to generate.
        seed:  Random seed for reproducibility.

    Returns:
        DataFrame with branch records.

    Data Engineering Note:
        Branches are a dimension table (dim_branch) in the star schema.
        They change slowly — a branch rarely moves or closes.
        This is an example of a Slowly Changing Dimension (SCD Type 1).
    """
    random.seed(seed)
    logger.info(f"Generating {count} branches...")

    records = []
    used_ids = set()

    for i in range(count):
        branch_id = _generate_branch_id(i, used_ids)
        used_ids.add(branch_id)

        city, state = random.choice(INDIAN_LOCATIONS)
        branch_type = random.choices(BRANCH_TYPES, weights=BRANCH_TYPE_WEIGHTS, k=1)[0]

        records.append(
            {
                "branch_id": branch_id,
                "branch_name": _generate_branch_name(city, branch_type, i),
                "city": city,
                "state": state,
                "branch_type": branch_type,
            }
        )

    df = pd.DataFrame(records)
    logger.info(f"Generated {len(df)} branch records.")
    return df


def _generate_branch_id(index: int, used_ids: set) -> str:
    """Generate a unique branch ID like BRN-001."""
    candidate = f"BRN-{index + 1:04d}"
    # Handle collision (unlikely but safe)
    while candidate in used_ids:
        candidate = f"BRN-{random.randint(1000, 9999)}"
    return candidate


def _generate_branch_name(city: str, branch_type: str, index: int) -> str:
    """Generate a realistic branch name."""
    suffixes = ["Main Branch", "City Branch", "Central Branch", "East Branch",
                "West Branch", "North Branch", "South Branch", "Corporate Hub",
                "Service Centre", "Extension Counter"]
    suffix = suffixes[index % len(suffixes)]
    return f"IDFC FIRST Bank - {city} {suffix}"
