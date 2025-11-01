{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('f1_raw_data', 'locations') }}
),

cleaned as (
    select
        -- Natural Keys
        session_key,
        meeting_key,
        driver_number,
        
        -- Timestamp
        date as location_timestamp,
        
        -- Coordinates (arbitrary origin point)
        x as position_x,
        y as position_y,
        z as position_z,
        
        -- Audit columns
        extracted_at as extracted_at

    from source
    where session_key is not null
        and driver_number is not null
        and date is not null
)

select * from cleaned