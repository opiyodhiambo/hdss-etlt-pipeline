-- models/intermediate/int_individuals.sql
{{ config(materialized="table") }}

with base as (
    select * from {{ ref("stg_mbita_core") }}
),

individuals as (
    select distinct
        individual_id,
        sex,
        date_of_birth,
        country_id,
        centre_id
    from base
)

select * from individuals
