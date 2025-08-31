-- models/intermediate/int_events.sql
{{ config(materialized="table") }}

with base as (
    select * from {{ ref("stg_mbita_core") }}
),

events as (
    select
        individual_id,
        event_number,
        event,
        event_date,
        observation_date
    from base
)

select * from events
