with source as (
select * from {{ source('bronze', 'bronze_returns') }}
),

cleaned as (
    select
        sales_id,

        date_sk,
        store_sk,
        product_sk,

        case when returned_qty > 0   then returned_qty                          else null end as returned_qty,
        case when refund_amount >= 0 then cast(refund_amount as numeric(12,2))  else null end as refund_amount,


        initcap(trim(return_reason))                       as return_reason,

        -- Return reason category grouping
        {{ return_category(return_reason) }}               as return_category,

        -- Unit refund per qty (useful for benchmarking)
        case
            when returned_qty > 0
            then round(cast(refund_amount as numeric(12,2)) / returned_qty, 2)
            else null
        end                                                as refund_per_unit

    from source
    where sales_id is not null
      and date_sk is not null
      and store_sk is not null
      and product_sk is not null
)

select * from cleaned