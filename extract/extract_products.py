import os
import json
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@{DB_HOST}:5432/"
    f"{os.getenv('POSTGRES_DB')}"
)

def extract_products() -> pd.DataFrame:
    """Pull the full product catalog from DummyJSON."""
    all_products = []
    limit = 30
    skip = 0

    while True:
        resp = requests.get(
            "https://dummyjson.com/products",
            params={"limit": limit, "skip": skip},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        products = data.get("products", [])
        if not products:
            break
        all_products.extend(products)
        skip += limit
        if skip >= data.get("total", 0):
            break

    df = pd.DataFrame(all_products)
    df["pulled_at"] = datetime.now(timezone.utc)
    return df

def load_to_postgres(df: pd.DataFrame):
    engine = create_engine(DB_URL)

    json_columns = ["dimensions", "meta", "reviews", "tags", "images"]
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if x is not None else None)

    dtype_map = {col: JSONB() for col in json_columns if col in df.columns}

    # Truncate-and-load instead of drop-and-recreate: this is a full
    # refresh of the product catalog snapshot, but unlike if_exists="replace",
    # it preserves the table object itself — which matters now that dbt has
    # built views (stg_products) directly on top of it. Dropping the table
    # would break those views; truncating just clears the rows.
    with engine.begin() as conn:
        table_exists = conn.execute(
            text("SELECT to_regclass('public.raw_products')")
        ).scalar()
        if table_exists:
            conn.execute(text("TRUNCATE TABLE public.raw_products"))

    df.to_sql(
        "raw_products",
        engine,
        schema="public",
        if_exists="append",
        index=False,
        dtype=dtype_map,
    )
    print(f"Loaded {len(df)} products into raw_products")

if __name__ == "__main__":
    df = extract_products()
    load_to_postgres(df)
