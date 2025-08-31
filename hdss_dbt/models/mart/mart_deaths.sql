-- models/marts/mart_deaths.sql
{{ config(materialized="table") }}

with events as (
    select * from {{ ref("int_events") }}
    where event = 'Death'
),

deaths as (
    select
        e.individual_id,
        e.event_date as death_date,
        i.date_of_birth,
        i.sex,
        i.centre_id,
        i.country_id
    from events e
    join {{ ref("int_individuals") }} i using (individual_id)
)

select * from deaths
