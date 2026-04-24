with source as (
    select * from {{ source('bronze', 'bronze_store') }}
),

cleaned as (
    select
        store_sk,
        store_code,

        initcap(trim(store_name))                          as store_name,
        initcap(trim(city))                                as city,
        upper(trim(state_province))                        as state_province,
        initcap(trim(region))                              as region,
        upper(trim(country))                               as country,

        cast(open_date as date)                            as open_date,


        case
            when sq_ft > 0 then sq_ft
            else null
        end                                                as sq_ft,

        case
            when sq_ft < 5000  then 'Small'
            when sq_ft < 10000 then 'Medium'
            when sq_ft < 20000 then 'Large'
            else 'Flagship'
        end                                                as store_size_category,


        initcap(trim(city)) || ', ' || upper(trim(state_province)) as store_location

    from source
    where store_sk is not null
      and store_code is not null
)

select * from cleaned