with source as (
    select * from {{ source('raw', 'raw_order_items') }}
),

renamed as (
    select
        id as order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price::numeric as unit_price,
        (quantity * unit_price)::numeric as line_total
    from source
)

select * from renamed
