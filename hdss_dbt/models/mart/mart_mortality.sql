-- models/marts/mart_mortality.sql
{{ config(materialized="table") }}

with deaths as (
    select * from {{ ref("mart_deaths") }}
),

age_at_death as (
    select
        individual_id,
        extract(year from age(death_date::date, date_of_birth::date)) as age_at_death,
        sex,
        centre_id,
        country_id
    from deaths
    where death_date is not null
      and date_of_birth is not null
)

select
    country_id,
    centre_id,
    sex,
    avg(age_at_death) as avg_age_at_death,
    count(*) as total_deaths
from age_at_death
group by country_id, centre_id, sex -- What is the average age at death, broken down by sex, for each centre in each country?
