-- models/marts/mart_births.sql
{{ config(materialized="table") }}

with events as (
    select * from {{ ref("int_events") }}
    where event = 'Births'
),

births as (
    select
        e.individual_id,
        e.event_date as birth_date,
        i.sex,
        i.centre_id,
        i.country_id
    from events e
    join {{ ref("int_individuals") }} i using (individual_id)
)

select * from births
