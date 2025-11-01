{{
    config(
        materialized='view',
        schema='staging'
    )
}}

select
  session_key,
  driver_number,
  full_name,
  team_name,
  extracted_at,
  extraction_id
from {{ source('f1_raw_data', 'drivers') }}
