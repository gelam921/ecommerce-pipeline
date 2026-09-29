# E-Commerce ELT Pipeline

An end-to-end data engineering pipeline that extracts e-commerce product and order data, loads it into Postgres, transforms it into a dimensional model with dbt, orchestrates the whole flow with Airflow, and serves the results through a live Streamlit dashboard — all containerized with Docker.

## Screenshots

**Live dashboard**
![Dashboard](screenshots/dashboard.png)

**Airflow DAG run**
![Airflow](screenshots/airflow_grid.png)

**dbt test output**
![dbt tests](screenshots/dbt_test.png)

## Why this project

Most portfolio pipelines stop at "pull an API into a database." This one goes further: it models the data into a proper star schema, tests data quality and referential integrity at every layer, and runs on a schedule against data that changes over time — closer to how a real analytics team's stack behaves than a one-off script.

## Architecture

```mermaid
flowchart LR
    subgraph Sources
        A[DummyJSON API]
        B[Faker-generated orders]
    end

    subgraph Bronze[Raw layer - Postgres]
        C[raw_products]
        D[raw_customers]
        E[raw_orders]
        F[raw_order_items]
    end

    subgraph Silver[dbt staging]
        G[stg_products]
        H[stg_customers]
        I[stg_orders]
        J[stg_order_items]
    end

    subgraph Gold[dbt marts]
        K[dim_products]
        L[dim_customers]
        M[fct_order_items]
    end

    N[Streamlit dashboard]
    O[Airflow - daily schedule]

    A --> C
    B --> D
    B --> E
    B --> F
    C --> G
    D --> H
    E --> I
    F --> J
    G --> K
    H --> L
    I --> M
    J --> M
    K --> N
    L --> N
    M --> N
    O -.orchestrates.-> A
    O -.orchestrates.-> B
    O -.orchestrates.-> Silver
```

## Stack

| Layer | Tool | Why |
|---|---|---|
| Extraction | Python (requests, Faker) | Pagination, retries, and synthetic order generation |
| Storage | PostgreSQL | Raw (bronze) and modeled (analytics) schemas |
| Transformation | dbt | Staging → marts, with 22 automated data quality tests |
| Orchestration | Airflow | Daily DAG with retries and task dependencies |
| Dashboard | Streamlit | Live KPIs and charts read directly from the marts |
| Containerization | Docker Compose | One command spins up the entire stack |

## Data model

Star schema at the order-item grain — the most granular reasonable level, so everything else (daily revenue, orders per customer, category performance) aggregates up from it rather than being locked in by an earlier, coarser design.

- **`dim_products`** — one row per product, with a computed `discounted_price`
- **`dim_customers`** — one row per customer
- **`fct_order_items`** — one row per line item, joined to order status and date, with a `net_line_total` that excludes cancelled orders

## Running it

```bash
git clone https://github.com/gelam921/ecommerce-pipeline.git
cd ecommerce-pipeline
cp .env.example .env
docker compose up -d --build
```

Then:
- **Airflow UI:** http://localhost:8080 — admin password is printed in `docker compose logs airflow` (search for `password:`)
- **Dashboard:** http://localhost:8501 — will show an error until the first DAG run completes; the DAG starts unpaused and runs automatically within about a minute

To trigger a run manually:
```bash
docker compose exec airflow airflow dags trigger ecommerce_elt_pipeline
```

To run dbt directly, outside the DAG:
```bash
docker compose run --rm dbt run
docker compose run --rm dbt test
```

## Data quality

22 dbt tests across staging and marts: uniqueness and not-null constraints on every primary key, plus relationship tests verifying every order item traces back to a real order, customer, and product. Run `docker compose run --rm dbt test` to see them execute.

## Performance

Added btree indexes on `fct_order_items.customer_id` and `.product_id` via dbt's model-level `indexes` config, so they're version-controlled and recreated automatically on every rebuild.

Tested against a bulk-loaded ~58,000-row fact table with a customer-lookup query:

```sql
EXPLAIN ANALYZE SELECT * FROM analytics.fct_order_items WHERE customer_id = 788813;
```
**Before (no index):**
Seq Scan on fct_order_items (cost=0.00..1559.86 rows=21 width=77) (actual time=0.018..2.456 rows=9 loops=1)  
Filter: (customer_id = 788813)  
Rows Removed by Filter: 58220  
Planning Time: 0.199 ms  
Execution Time: 2.477 ms

**After (btree index on customer_id):**
Bitmap Heap Scan on fct_order_items (cost=4.45..78.71 rows=21 width=77) (actual time=0.520..0.529 rows=9 loops=1  
Recheck Cond: (customer_id = 788813)  
Heap Blocks: exact=2  
-> Bitmap Index Scan on fct_order_items_customer_id_idx (cost=0.00..4.45 rows=21 width=0) (actual time=0.488..0.488 rows=9 loops=1)  
Index Cond: (customer_id = 788813)  
Planning Time: 0.815 ms  
Execution Time: 1.724 ms

The gap is modest at this scale since the table still fits in memory — a sequential scan's cost grows linearly with table size, while an index lookup's cost grows far more slowly, so the difference would be dramatically larger at production scale (millions of rows).

Also confirmed indexing isn't universally helpful: a date-range filter matching ~94% of rows (non-selective) showed no meaningful benefit, consistent with how Postgres's query planner chooses when to actually use an index versus scan sequentially.

## Notable bugs and fixes (why this wasn't a straight-line build)

- **JSONB silently stored as text** — a bare SQLAlchemy type class (`JSONB`) instead of an instance (`JSONB()`) caused pandas to fall back to text inference; dbt's `jsonb`-only `->>` operator caught the mismatch at build time.
- **Non-idempotent primary keys** — `order_item_id` was a per-run counter starting at 1, causing duplicate keys on every second pipeline run. Fixed by deriving the next ID from `MAX(id)` in the existing table.
- **Drop-and-recreate broke downstream views** — once dbt built a view on top of `raw_products`, the extraction script's `if_exists="replace"` pattern (a `DROP TABLE`) started failing because Postgres won't drop a table with dependent views. Switched to truncate-and-load, the correct pattern for a full-refresh source with dependents.

## Known simplifications (and what production would use instead)

- **Airflow `standalone` mode** (SQLite metadata DB, single process) instead of the Postgres + CeleryExecutor setup a production deployment would use for parallelism and durability.
- **Full-refresh staging models** instead of incremental dbt models — fine at this data volume, but would need `is_incremental()` logic at real scale.
- **No secrets manager** — credentials are handled via `.env` / `env_var()`, appropriate for a local project; production would use something like AWS Secrets Manager or Vault.

## Project structure
├── docker-compose.yml  
├── Dockerfile.dbt  
├── Dockerfile.airflow  
├── Dockerfile.streamlit  
├── extract/  
 │ ├── extract_products.py  
 │ └── generate_orders.py  
├── dbt_project/  
│ ├── models/staging/  
│ ├── models/marts/  
│ └── profiles.yml  
├── dags/  
│ └── ecommerce_pipeline_dag.py  
└── dashboard/  
└── app.py
