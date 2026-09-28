with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        oi.product_id,
        o.order_date,
        o.order_status,
        oi.quantity,
        oi.unit_price,
        oi.line_total,
        case
            when o.order_status = 'cancelled' then 0
            else oi.line_total
        end as net_line_total
    from order_items oi
    left join orders o on oi.order_id = o.order_id
)

select * from final
