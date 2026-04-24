with source as (
    select * from {{ source('bronze', 'bronze_data') }}
),

cleaned as (
    select
        date_sk,

        cast(date as date)                                   as date,

        day                                                  as day,
        date_format(cast(date as date), 'EEEE')              as day_name,

        month(cast(date as date))                            as month,
        date_format(cast(date as date), 'MMMM')              as month_name,

        year(cast(date as date))                             as year,
        quarter(cast(date as date))                          as quarter,

        left(initcap(date_format(cast(date as date), 'MMMM')), 3)
            || ' ' || cast(year(cast(date as date)) as string)
                                                             as month_year_label,

        cast(year(cast(date as date)) as string) || '-W' ||
        lpad(
            cast(weekofyear(cast(date as date)) as string),
            2, '0'
        )                                                    as iso_week_label,

        case
            when dayofweek(cast(date as date)) in (1, 7) then 'Weekend'
            else 'Weekday'
        end                                                  as day_type,

        case
            when dayofweek(cast(date as date)) in (1, 7) then true
            else false
        end                                                  as is_weekend

    from source
    where date_sk is not null
)

select * from cleaned