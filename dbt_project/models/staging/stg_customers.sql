with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

renamed as (
    select
        id as customer_id,
        name as customer_name,
        email,
        country,
        created_at
    from source
)

select * from renamed
