import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST', 'localhost')}:5432/"
    f"{os.getenv('POSTGRES_DB')}"
)


@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


@st.cache_data(ttl=60)
def query(sql: str) -> pd.DataFrame:
    return pd.read_sql(sql, get_engine())


st.set_page_config(page_title="E-commerce Analytics", layout="wide")
st.title("E-commerce Analytics")
st.caption("Source: analytics marts built by dbt, orchestrated by Airflow")

kpis = query("""
    select
        round(sum(net_line_total), 2) as net_revenue,
        count(distinct order_id) as orders,
        count(distinct customer_id) as customers,
        round(100.0 * count(distinct order_id) filter (where order_status = 'cancelled')
              / count(distinct order_id), 1) as cancellation_rate
    from analytics.fct_order_items
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Net revenue", f"${kpis['net_revenue']:,.0f}")
c2.metric("Orders", int(kpis["orders"]))
c3.metric("Customers", int(kpis["customers"]))
c4.metric("Cancellation rate", f"{kpis['cancellation_rate']}%")

left, right = st.columns(2)

with left:
    st.subheader("Net revenue by category (top 10)")
    revenue = query("""
        select p.category, round(sum(f.net_line_total), 2) as net_revenue
        from analytics.fct_order_items f
        join analytics.dim_products p using (product_id)
        group by 1 order by 2 desc limit 10
    """)
    st.bar_chart(revenue.set_index("category"))

with right:
    st.subheader("Units sold by category (top 10)")
    units = query("""
        select p.category,
               sum(case when f.order_status <> 'cancelled' then f.quantity else 0 end) as units
        from analytics.fct_order_items f
        join analytics.dim_products p using (product_id)
        group by 1 order by 2 desc limit 10
    """)
    st.bar_chart(units.set_index("category"))

left2, right2 = st.columns(2)

with left2:
    st.subheader("Orders per day")
    daily = query("""
        select date_trunc('day', order_date)::date as day,
               count(distinct order_id) as orders
        from analytics.fct_order_items
        group by 1 order by 1
    """)
    st.line_chart(daily.set_index("day"))

with right2:
    st.subheader("Orders by status")
    status = query("""
        select order_status, count(distinct order_id) as orders
        from analytics.fct_order_items
        group by 1 order by 2 desc
    """)
    st.bar_chart(status.set_index("order_status"))
