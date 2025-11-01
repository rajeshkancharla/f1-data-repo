{{ config(materialized='table') }}

select
  driver_number,
  name,
  nationality,
  team_name,
  max(extracted_at) as last_updated
from {{ ref('stg_drivers') }}
group by 1, 2, 3, 4
