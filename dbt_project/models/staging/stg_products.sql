with source as (
    select * from {{ source('raw', 'raw_products') }}
),

renamed as (
    select
        id as product_id,
        title as product_title,
        category,
        brand,
        sku,
        price::numeric as price,
        "discountPercentage"::numeric as discount_percentage,
        rating::numeric as rating,
        stock::int as stock_quantity,
        "availabilityStatus" as availability_status,
        (dimensions::jsonb ->> 'width')::numeric as width_cm,
        (dimensions::jsonb ->> 'height')::numeric as height_cm,
        (dimensions::jsonb ->> 'depth')::numeric as depth_cm,
        pulled_at
    from source
)

select * from renamed
