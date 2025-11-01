{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('f1_raw_data', 'pit') }}
),

cleaned as (
    select
        -- Natural Keys
        session_key,
        meeting_key,
        driver_number,
        lap_number,
        
        -- Timestamp
        date as pit_stop_time,
        
        -- Performance Metrics
        pit_duration,
        
        -- Audit columns
        extracted_at as extracted_at

    from source
    where session_key is not null
        and driver_number is not null
)

select * from cleaned