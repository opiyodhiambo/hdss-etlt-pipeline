{{config(
    materialized='table'
)}}


with source_data as (
    select
        upper(sex) as sex,
        year::int as year,
        agecat_1 as age_category,
        deaths::int as deaths,
        "PYO"::float as person_years_observed,
        births::int as births
    from {{source('asmr', 'asmr')}}
),

normalized as (
    select 
        case
            when sex in ('f', 'F', 'female') then 'F'
            when sex in ('M', 'm', 'male') then 'M'
            else sex
        end as sex,
        year,
        age_category,
        deaths,
        person_years_observed,
        births
    from source_data 
),

filtered as (
    select * 
    from normalized
    where age_category %1 = 0 
)

select * from filtered