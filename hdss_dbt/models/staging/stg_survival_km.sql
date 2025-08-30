{{ config(
    materialized='table'
) }}

with source_data as (
    select
        case
            when upper(sex) in ('F', 'FEMALE') then 'F'
            when upper(sex) in ('M', 'MALE') then 'M'
            else upper(sex)
        end as sex,
        y_bloc,
        time,
        survivor as survival_probability
    from {{ source ('survival_km', 'survival_km') }}
),

converted as (
    select
        sex,
        y_bloc,
        time * 12 as months_elapsed,       -- convert fraction of year to months
        time as years_elapsed,
        survival_probability
    from source_data
)

select * from converted
