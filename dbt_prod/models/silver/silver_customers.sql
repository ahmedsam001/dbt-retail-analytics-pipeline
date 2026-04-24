with bronze as (
    select * from {{ source('bronze', 'bronze_customer') }}
),

cleaned as (
    select
        customer_sk,
        customer_code,

        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name))  as last_name,
        initcap(trim(first_name)) || ' ' ||
            initcap(trim(last_name))  as full_name,

        case
            when upper(trim(gender)) in ('M', 'MALE')   then 'Male'
            when upper(trim(gender)) in ('F', 'FEMALE') then 'Female'
            else 'Unknown'
        end as gender,

        trim(email) as email,
        regexp_replace(trim(phone), '[^0-9+\-]', '') as phone,

        initcap(trim(loyalty_tier))  as loyalty_tier,

        cast(signup_date as date)  as signup_date,


        case
            when upper(trim(loyalty_tier)) in ('GOLD', 'PLATINUM') then true
            else false
        end   as is_premium_customer

    from bronze
    where customer_sk is not null
      and customer_code is not null
)

select * from cleaned