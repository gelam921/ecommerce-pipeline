import os
import random
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from faker import Faker
from datetime import datetime, timezone, timedelta

load_dotenv()
fake = Faker()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@{DB_HOST}:5432/"
    f"{os.getenv('POSTGRES_DB')}"
)

NUM_NEW_CUSTOMERS = 15
NUM_NEW_ORDERS = 40
MAX_ITEMS_PER_ORDER = 4

ORDER_STATUSES = ["delivered", "shipped", "pending", "cancelled"]
ORDER_STATUS_WEIGHTS = [0.6, 0.2, 0.15, 0.05]


def get_existing_product_ids(engine) -> list[int]:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, price FROM raw_products"))
        return [(row.id, float(row.price)) for row in result]

def get_next_order_item_id(engine) -> int:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM raw_order_items"))
        return result.scalar() + 1

def generate_customers(n: int) -> pd.DataFrame:
    customers = []
    for _ in range(n):
        customers.append(
            {
                "id": fake.unique.random_int(min=100000, max=999999),
                "name": fake.name(),
                "email": fake.unique.email(),
                "country": fake.country(),
                "created_at": fake.date_time_between(
                    start_date="-1y", end_date="now", tzinfo=timezone.utc
                ),
            }
        )
    return pd.DataFrame(customers)


def generate_orders_and_items(
    customer_ids: list[int], products: list[tuple[int, float]], n_orders: int, starting_order_item_id: int
):
    orders, order_items = [], []
    order_item_id = starting_order_item_id

    for _ in range(n_orders):
        order_id = fake.unique.random_int(min=1000000, max=9999999)
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(
            start_date="-30d", end_date="now", tzinfo=timezone.utc
        )
        status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

        orders.append(
            {
                "id": order_id,
                "customer_id": customer_id,
                "order_date": order_date,
                "status": status,
            }
        )

        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        chosen_products = random.sample(products, k=min(num_items, len(products)))

        for product_id, price in chosen_products:
            order_items.append(
                {
                    "id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": random.randint(1, 3),
                    "unit_price": price,
                }
            )
            order_item_id += 1

    return pd.DataFrame(orders), pd.DataFrame(order_items)


def load_append(df: pd.DataFrame, table_name: str, engine):
    df.to_sql(table_name, engine, schema="public", if_exists="append", index=False)
    print(f"Appended {len(df)} rows into {table_name}")


if __name__ == "__main__":
    engine = create_engine(DB_URL)

    products = get_existing_product_ids(engine)
    if not products:
        raise RuntimeError("No products found in raw_products — run extract_products.py first.")

    customers_df = generate_customers(NUM_NEW_CUSTOMERS)

    starting_order_item_id = get_next_order_item_id(engine)
    orders_df, order_items_df = generate_orders_and_items(
        customer_ids=customers_df["id"].tolist(),
        products=products,
        n_orders=NUM_NEW_ORDERS,
        starting_order_item_id=starting_order_item_id,
    )

    load_append(customers_df, "raw_customers", engine)
    load_append(orders_df, "raw_orders", engine)
    load_append(order_items_df, "raw_order_items", engine)