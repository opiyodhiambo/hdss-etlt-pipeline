-- models/staging/stg_seasonality.sql
{{ config(
    materialized='table'
) }}

SELECT *
FROM {{ ref('raw_seasonality') }}
-- TODO: transformations for seasonality
