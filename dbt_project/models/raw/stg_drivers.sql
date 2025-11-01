{{ config(materialized='view') }}

select
  driver_number,
  name,
  team_name,
  nationality,
  extracted_at,
  extraction_id
from {{ source('f1_raw_data', 'drivers') }}
