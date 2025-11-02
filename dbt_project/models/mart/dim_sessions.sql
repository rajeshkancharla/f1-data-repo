{{
    config(
        materialized='table',
        schema='mart'
    )
}}

-- Note: This assumes you have a sessions table in your raw layer
-- If not available, you can derive session info from the laps/drivers tables

with session_info as (
    select distinct
        session_key,
        meeting_key
    from {{ ref('stg_laps') }}
),

final as (
    select
        -- Surrogate Key
        {{ dbt_utils.generate_surrogate_key(['session_key']) }} as session_dim_key,
        
        -- Natural Key
        session_key,
        meeting_key,
        
        -- Session Attributes
        -- Note: Add these columns if you extract sessions endpoint data
        -- session_name,
        -- session_type,
        -- date_start,
        -- date_end,
        -- circuit_short_name,
        -- location,
        -- country_name,
        
        -- Audit
        current_timestamp() as dw_created_at,
        current_timestamp() as dw_updated_at
        
    from session_info
)

select * from final