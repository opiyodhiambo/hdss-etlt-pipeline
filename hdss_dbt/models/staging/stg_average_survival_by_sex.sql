{{config(
    materialized='table'
)}}

with source_data as (
    select
        year::int as year,
        upper(sex) as sex,
        p50::float as median_survival
    from {{source('average_survival_by_sex', 'average_survival_by_sex')}}
),

normalized as (
    select
        year, 
        case
            when sex in ('f', 'F', 'female') then 'F'
            when sex in ('M', 'm', 'male') then 'M'
            else sex
        end as sex,
        median_survival
    from source_data
)

select * from normalized