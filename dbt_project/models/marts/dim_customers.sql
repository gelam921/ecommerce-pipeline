with customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        customer_id,
        customer_name,
        email,
        country,
        created_at as customer_since
    from customers
)

select * from final
