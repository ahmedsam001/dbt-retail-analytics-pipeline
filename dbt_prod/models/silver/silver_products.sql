with source as (
    select * from {{ source('bronze', 'bronze_product') }}
),

cleaned as (
    select
        product_sk,
        product_code,

        initcap(trim(product_name))   as product_name,
        initcap(trim(department))     as department,
        initcap(trim(category))       as category,
        supplier_sk,

        case
            when list_price is null then 'Unknown'
            when list_price < 5    then 'Budget'
            when list_price < 20   then 'Mid-Range'
            when list_price < 100  then 'Premium'
            else 'Luxury'
        end as price_tier,

        lower(trim(uom))  as uom


    from source
    where product_sk is not null
      and product_code is not null
)

select * from cleaned