{{
    config(
        materialized='table',
        schema='mart'
    )
}}
select
  d.driver_number,
  d.full_name,
  count(l.lap_number) as total_laps,
  avg(l.lap_duration) as avg_lap_time,
  min(l.lap_duration) as best_lap_time
from {{ ref('stg_laps') }} l
join {{ ref('stg_drivers') }} d
  on l.driver_number = d.driver_number
group by 1, 2
