-- models/staging/stg_survival_summary.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ ref('raw_survival_summary') }}
-- TODO: transformations from File 2
