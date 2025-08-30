{{ config(
    materialized='table'
) }}

with source_data as (
    select
        year::int as year,
        month::int as month,
        agecat_1::int as age_category,
        deaths::int as deaths,
        "PYO"::float as person_years_observed,
        y_bloc
    from {{ source('seasonality', 'seasonality') }}
),

filtered as (
    select *
    from source_data
    where age_category % 1 = 0  
)

select * from filtered
