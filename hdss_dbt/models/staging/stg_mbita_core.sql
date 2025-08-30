{{ config(
    materialized='table'
) }}

with source_data as (
    select *
    from {{ source('mbita_core', 'mbita_core') }}
),

staged as (
    select
        "RecNr" as rec_number,
        "CountryId" as country_id,
        "CentreId" as centre_id,
        "IndividualId" as individual_id,
        case   
            when "Sex" = 1 then 'M'
            when "Sex" = 2 then 'F'
            else 'Unknown'
        end as sex,
        "DoB" as date_of_birth,
        "EventNr" as event_count,
        "EventCode" as event_number,
        case "EventDate" 
            when 'BTH' then 'Births'
            when 'DLV' then 'Delivery'
            when 'DTH' then 'Death'
            when 'ENT' then 'Entry'
            when 'ENU' then 'Enumeration'
            when 'EXT' then 'Exit'
            when 'IMG' then 'Immigration'
            when 'OBE' then 'Observation enddate'
            when 'OBL' then 'Last Observation'
            when 'OBS' then 'Observation'
            when 'OMG' then 'Outmigration'
            else 'Unknown'
        end as event_code,
        "ObservationDate" as event_date,
        "MotherId" as observation_date
    from source_data
)

select * from staged