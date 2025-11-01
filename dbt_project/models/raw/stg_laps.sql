{{ config(materialized='view') }}

select
  session_key,
  driver_number,
  lap_number,
  duration as lap_time,
  position,
  extracted_at
from {{ source('f1_raw_data', 'laps') }}
