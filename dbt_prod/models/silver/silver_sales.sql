-- silver/stg_sales.sql
-- Cleaned, validated, and enriched sales fact table

with source as (
    select * from {{ source('bronze', 'bronze_sales') }}
),

cleaned as (
    select
        sales_id,


        date_sk,
        store_sk,
        product_sk,
        customer_sk,
        promotion_sk,

        case when quantity > 0         then quantity         else null end as quantity,
        case when unit_price > 0       then cast(unit_price as numeric(10,2))      else null end as unit_price,
        case when gross_amount > 0     then cast(gross_amount as numeric(12,2))    else null end as gross_amount,
        case when discount_amount >= 0 then cast(discount_amount as numeric(12,2)) else null end as discount_amount,
        case when net_amount > 0       then cast(net_amount as numeric(12,2))      else null end as net_amount,

        -- Standardize payment method
        initcap(trim(payment_method))                      as payment_method,

        -- Derived measures
        {{ discount_pct('discount_amount', 'gross_amount') }} as discount_pct,
 

        -- Flag transactions with unusually high discount (> 50%)
        {{ high_discount('discount_amount', 'gross_amount') }} as is_high_discount,


        -- Promotion flag
        case
            when promotion_sk is not null                  then true
            else false
        end                                                as has_promotion

    from source
    where sales_id is not null
      and date_sk is not null
      and store_sk is not null
      and product_sk is not null
      and customer_sk is not null
)

select * from cleaned