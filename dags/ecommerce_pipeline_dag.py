from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import psycopg2
import os

def log_failure_alert(context):
    """Writes a row to public.pipeline_alerts when a task fails after
    exhausting retries. Self-contained — no external service required."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    task_id = task_instance.task_id
    execution_date = context.get("execution_date")
    log_url = task_instance.log_url

    conn = None
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            dbname=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public.pipeline_alerts (
                    id SERIAL PRIMARY KEY,
                    dag_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    execution_date TIMESTAMPTZ,
                    log_url TEXT,
                    failed_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            cur.execute(
                """
                INSERT INTO public.pipeline_alerts
                    (dag_id, task_id, execution_date, log_url)
                VALUES (%s, %s, %s, %s)
                """,
                (dag_id, task_id, execution_date, log_url),
            )
        conn.commit()
    except Exception as e:
        # A logging failure shouldn't cascade into a second error on top
        # of the original task failure — print and move on.
        print(f"Failed to log alert: {e}")
    finally:
        if conn:
            conn.close()

default_args = {
    "owner": "data-eng-portfolio",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": log_failure_alert,
}

with DAG(
    dag_id="ecommerce_elt_pipeline",
    default_args=default_args,
    description="Extract product/order data, load to Postgres, transform with dbt",
    schedule_interval="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["ecommerce", "elt"],
) as dag:

    extract_products = BashOperator(
        task_id="extract_products",
        bash_command="python /opt/airflow/extract/extract_products.py",
    )

    generate_orders = BashOperator(
        task_id="generate_orders",
        bash_command="python /opt/airflow/extract/generate_orders.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    """ dbt_run won't even attempt to start until both extraction steps finish successfully; dbt_test won't run against half-loaded data"""
    extract_products >> generate_orders >> dbt_run >> dbt_test
