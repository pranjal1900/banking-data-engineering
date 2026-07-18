"""
Banking Data Engineering Platform — Merchant Generator
=======================================================
Generates synthetic merchant/vendor data with risk categories.

Why merchant risk categories?
  In fraud detection, transactions at HIGH-risk merchants
  (e.g., certain electronics or international travel merchants)
  are weighted more heavily in the fraud scoring engine.
  This is a real pattern used in banking fraud systems.

Interview talking point:
  "Merchants are a dimension table with a risk_category attribute.
  This attribute is used as a feature in our rule-based fraud
  detection — transactions at HIGH-risk merchants trigger
  additional scrutiny even if the amount is moderate."
"""

import random
import logging
import pandas as pd
from faker import Faker

logger = logging.getLogger("banking.ingestion.merchants")
fake = Faker("en_IN")

MERCHANT_CATEGORIES = [
    "Grocery",
    "Travel",
    "Restaurant",
    "Electronics",
    "Healthcare",
    "Education",
    "Entertainment",
    "Fuel",
    "Shopping",
    "Utilities",
]

# Risk categories by merchant type — reflects real fraud patterns.
# Electronics and Travel are higher risk due to high-value transactions
# and common fraud vectors (resale, chargebacks).
CATEGORY_RISK_MAP = {
    "Grocery": "LOW",
    "Travel": "HIGH",
    "Restaurant": "LOW",
    "Electronics": "HIGH",
    "Healthcare": "LOW",
    "Education": "LOW",
    "Entertainment": "MEDIUM",
    "Fuel": "MEDIUM",
    "Shopping": "MEDIUM",
    "Utilities": "LOW",
}

# Realistic merchant name templates per category
MERCHANT_NAME_TEMPLATES = {
    "Grocery": ["BigBazaar", "Reliance Fresh", "D-Mart", "More Supermarket", "Spencer's"],
    "Travel": ["MakeMyTrip", "Cleartrip", "IRCTC", "IndiGo", "Air India", "OYO"],
    "Restaurant": ["Zomato", "Swiggy", "McDonald's", "Domino's", "KFC", "Haldiram's"],
    "Electronics": ["Croma", "Reliance Digital", "Amazon India", "Flipkart", "Vijay Sales"],
    "Healthcare": ["Apollo Pharmacy", "MedPlus", "NetMeds", "1mg", "PharmEasy"],
    "Education": ["BYJU's", "Unacademy", "Coursera", "Udemy", "NIIT"],
    "Entertainment": ["BookMyShow", "PVR Cinemas", "Netflix India", "Hotstar", "Amazon Prime"],
    "Fuel": ["Indian Oil", "BPCL", "HPCL", "Bharat Petroleum", "Essar Fuel"],
    "Shopping": ["Myntra", "Ajio", "Nykaa", "Lenskart", "Pepperfry"],
    "Utilities": ["Tata Power", "BESCOM", "MSEDCL", "Bharat Gas", "Indane Gas"],
}

INDIAN_CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Chennai", "Kolkata",
    "Hyderabad", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
    "Surat", "Kochi", "Noida", "Gurgaon", "Bhopal",
]


def generate_merchants(count: int = 500, seed: int = 42) -> pd.DataFrame:
    """
    Generate synthetic merchant data.

    Args:
        count: Number of merchants to generate.
        seed:  Random seed for reproducibility.

    Returns:
        DataFrame with merchant records.

    Design decision:
        We use a category→risk mapping rather than random risk assignment.
        This creates realistic co-variation between category and risk,
        which makes fraud detection patterns meaningful (not random noise).
    """
    random.seed(seed)
    logger.info(f"Generating {count} merchants...")

    records = []
    used_ids = set()

    for i in range(count):
        merchant_id = _generate_merchant_id(i, used_ids)
        used_ids.add(merchant_id)

        category = random.choice(MERCHANT_CATEGORIES)
        risk_category = CATEGORY_RISK_MAP[category]

        # Pull from category-specific names, with numeric suffix for uniqueness
        base_names = MERCHANT_NAME_TEMPLATES[category]
        base_name = random.choice(base_names)
        # Add location + number to avoid duplicates
        city = random.choice(INDIAN_CITIES)
        merchant_name = f"{base_name} - {city} #{i + 1}"

        records.append(
            {
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "merchant_category": category,
                "city": city,
                "risk_category": risk_category,
            }
        )

    df = pd.DataFrame(records)
    logger.info(
        f"Generated {len(df)} merchants | "
        f"HIGH={len(df[df.risk_category=='HIGH'])} | "
        f"MEDIUM={len(df[df.risk_category=='MEDIUM'])} | "
        f"LOW={len(df[df.risk_category=='LOW'])}"
    )
    return df


def _generate_merchant_id(index: int, used_ids: set) -> str:
    """Generate a unique merchant ID like MER-00001."""
    candidate = f"MER-{index + 1:05d}"
    while candidate in used_ids:
        candidate = f"MER-{random.randint(10000, 99999)}"
    return candidate
