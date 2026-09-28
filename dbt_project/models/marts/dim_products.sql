with products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        product_id,
        product_title,
        category,
        brand,
        sku,
        price,
        discount_percentage,
        round(price * (1 - discount_percentage / 100), 2) as discounted_price,
        rating,
        stock_quantity,
        availability_status,
        width_cm,
        height_cm,
        depth_cm
    from products
)

select * from final
