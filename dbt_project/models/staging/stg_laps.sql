{{
    config(
        materialized='view',
        schema='staging'
    )
}}

select
  session_key,
  meeting_key,
  driver_number,
  lap_number,
  lap_duration,
  extraction_id,
  extracted_at
from {{ source('f1_raw_data', 'laps') }}
