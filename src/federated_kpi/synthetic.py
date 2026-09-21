from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import List

import numpy as np
import pandas as pd

from .core import LocalParty


CITY_ROWS = [
    ("Vienna", "Vienna", "Austria"),
    ("Graz", "Styria", "Austria"),
    ("Leoben", "Styria", "Austria"),
    ("Linz", "Upper Austria", "Austria"),
    ("Wels", "Upper Austria", "Austria"),
    ("Salzburg", "Salzburg", "Austria"),
    ("Innsbruck", "Tyrol", "Austria"),
    ("Kufstein", "Tyrol", "Austria"),
    ("Klagenfurt", "Carinthia", "Austria"),
    ("Villach", "Carinthia", "Austria"),
    ("St. Polten", "Lower Austria", "Austria"),
    ("Wiener Neustadt", "Lower Austria", "Austria"),
    ("Eisenstadt", "Burgenland", "Austria"),
    ("Bregenz", "Vorarlberg", "Austria"),
    ("Dornbirn", "Vorarlberg", "Austria"),
]

CATEGORIES = {
    "Electronics": ["Laptop", "Tablet", "Headphones", "Monitor", "Router", "Keyboard"],
    "Home": ["Lamp", "Chair", "Desk", "Kettle", "Vacuum", "Mixer"],
    "Sports": ["Bike Helmet", "Yoga Mat", "Dumbbell", "Running Shoes", "Backpack", "Bottle"],
    "Books": ["Data Science Book", "Novel", "Cookbook", "Travel Guide", "History Book", "Notebook"],
    "Office": ["Printer", "Paper", "Pen Set", "Webcam", "Dock", "Mouse"],
}


def _age_band(age: int) -> str:
    if age < 25:
        return "18-24"
    if age < 40:
        return "25-39"
    if age < 60:
        return "40-59"
    return "60+"


def create_federation(
    root: Path,
    n_parties: int = 4,
    customers_per_party: int = 450,
    facts_per_party: int = 5000,
    seed: int = 42,
) -> List[LocalParty]:
    root.mkdir(parents=True, exist_ok=True)
    rng_master = np.random.default_rng(seed)
    parties: List[LocalParty] = []

    products = []
    pk = 1
    category_base_price = {
        "Electronics": 260.0,
        "Home": 80.0,
        "Sports": 65.0,
        "Books": 24.0,
        "Office": 95.0,
    }
    for category, names in CATEGORIES.items():
        for name in names:
            products.append((pk, name, category, "ALL", category_base_price[category]))
            pk += 1
    product_df = pd.DataFrame(
        products, columns=["product_key", "product_name", "category", "all_product", "base_price"]
    )

    dates = pd.date_range("2025-01-01", "2025-12-31", freq="D")
    date_df = pd.DataFrame({"date_key": np.arange(1, len(dates) + 1), "date": dates})
    date_df["month"] = date_df["date"].dt.strftime("%Y-%m")
    date_df["quarter"] = date_df["date"].dt.to_period("Q").astype(str)
    date_df["year"] = date_df["date"].dt.year.astype(int)
    date_df["all_time"] = "ALL"
    date_df["date"] = date_df["date"].astype(str)

    for i in range(n_parties):
        rng = np.random.default_rng(int(rng_master.integers(0, 2**31 - 1)))
        party_id = f"P{i+1}"
        db_path = root / f"{party_id}.sqlite"
        if db_path.exists():
            db_path.unlink()

        # Each party has a different geographic concentration, creating realistic sparsity.
        city_weights = np.full(len(CITY_ROWS), 0.2)
        primary = [(i * 3 + j) % len(CITY_ROWS) for j in range(5)]
        city_weights[primary] += np.array([4.0, 2.8, 2.0, 1.3, 0.8])
        city_weights = city_weights / city_weights.sum()
        city_idx = rng.choice(len(CITY_ROWS), size=customers_per_party, p=city_weights)

        ages = np.clip(np.round(rng.normal(43 + (i - 1.5) * 2.0, 14, customers_per_party)), 18, 85).astype(int)
        customer_rows = []
        for c in range(customers_per_party):
            city, state, country = CITY_ROWS[int(city_idx[c])]
            age = int(ages[c])
            customer_rows.append(
                (
                    c + 1,
                    city,
                    state,
                    country,
                    "ALL",
                    age,
                    f"{(age // 10) * 10}-{(age // 10) * 10 + 9}",
                    _age_band(age),
                    "ALL",
                )
            )
        customer_df = pd.DataFrame(
            customer_rows,
            columns=[
                "customer_key", "city", "state", "country", "all_geo",
                "age", "age_decade", "age_band", "all_age",
            ],
        )

        # Product preferences differ by party and create cross-party heterogeneity.
        product_weights = np.ones(len(product_df), dtype=float)
        favored_category = list(CATEGORIES.keys())[i % len(CATEGORIES)]
        product_weights[product_df["category"].to_numpy() == favored_category] *= 3.2
        product_weights /= product_weights.sum()

        # Customer purchasing frequency is skewed; a few customers are heavy buyers.
        cust_weights = rng.lognormal(mean=0.0, sigma=0.8, size=customers_per_party)
        cust_weights /= cust_weights.sum()
        fact_customer = rng.choice(np.arange(1, customers_per_party + 1), size=facts_per_party, p=cust_weights)
        fact_product = rng.choice(product_df["product_key"].to_numpy(), size=facts_per_party, p=product_weights)
        # Mild seasonal skew.
        day_weights = 1.0 + 0.25 * np.sin(np.linspace(0, 2 * np.pi, len(date_df), endpoint=False))
        day_weights = day_weights / day_weights.sum()
        fact_date = rng.choice(date_df["date_key"].to_numpy(), size=facts_per_party, p=day_weights)
        quantity = rng.choice([1, 2, 3, 4], size=facts_per_party, p=[0.68, 0.21, 0.08, 0.03])
        base_lookup = product_df.set_index("product_key")["base_price"].to_dict()
        base = np.array([base_lookup[int(x)] for x in fact_product], dtype=float)
        revenue = base * quantity * rng.lognormal(mean=0.0, sigma=0.30, size=facts_per_party)
        fact_df = pd.DataFrame(
            {
                "sale_key": np.arange(1, facts_per_party + 1),
                "customer_key": fact_customer,
                "product_key": fact_product,
                "date_key": fact_date,
                "quantity": quantity.astype(int),
                "revenue": np.round(revenue, 2),
            }
        )

        with sqlite3.connect(db_path) as conn:
            customer_df.to_sql("dim_customer", conn, index=False)
            product_df.drop(columns=["base_price"]).to_sql("dim_product", conn, index=False)
            date_df.to_sql("dim_date", conn, index=False)
            fact_df.to_sql("fact_sales", conn, index=False)
            conn.executescript(
                """
                CREATE INDEX idx_fact_customer ON fact_sales(customer_key);
                CREATE INDEX idx_fact_product ON fact_sales(product_key);
                CREATE INDEX idx_fact_date ON fact_sales(date_key);
                CREATE INDEX idx_customer_geo ON dim_customer(city, state, country);
                CREATE INDEX idx_customer_age ON dim_customer(age, age_decade, age_band);
                CREATE INDEX idx_product_category ON dim_product(category);
                CREATE INDEX idx_date_year ON dim_date(year);
                """
            )

        parties.append(LocalParty(party_id, db_path))
    return parties
