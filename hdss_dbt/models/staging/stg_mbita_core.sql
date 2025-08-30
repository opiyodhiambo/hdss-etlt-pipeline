-- -- models/staging/stg_mbita_core.sql
-- {{ config(
--     materialized='table'
-- ) }}

-- SELECT *
-- FROM {{ ref('raw_mbita_core') }}
-- -- TODO: clean date formats, harmonize Sex codes, rename fields
