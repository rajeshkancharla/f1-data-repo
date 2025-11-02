{{
    config(
        materialized='table',
        schema='mart'
    )
}}

with driver_latest as (
    select
        driver_number,       
        full_name,
        team_name,
        row_number() over (
            partition by driver_number 
            order by extracted_at desc
        ) as rn
    from {{ ref('stg_drivers') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['driver_number']) }} as driver_key,
        
        -- Natural Key
        driver_number,
        
        -- Driver Attributes
        full_name,
        
        -- Team Attributes (current team - SCD Type 1)
        team_name,
                
        -- Audit
        current_timestamp() as dw_created_at,
        current_timestamp() as dw_updated_at
        
    from driver_latest
    where rn = 1
)

select * from final