from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng-portfolio",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
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
