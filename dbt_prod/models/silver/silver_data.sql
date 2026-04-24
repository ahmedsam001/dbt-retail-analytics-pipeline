with source as (
    select * from {{ source('bronze', 'bronze_data') }} 
),

cleaned as (
    select
        date_sk,
        cast(date as date) as date,
        day,

        cast(year as string) || '-W' ||
        lpad(
            cast(extract(week from cast(date as date)) as string),
            2, '0'
        ) as iso_week_label,

        left(initcap(month_name), 3) || ' ' || cast(year as string) as month_year_label

    from source
    where date_sk is not null
)

select * from cleaned