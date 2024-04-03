{{ config(materialized='view') }}

with source_data as (
    select code as event_code, 
           label as event_name
    from {{ env_var('DATASET') }}.lookups
    where table='eventcodes'
)
select event_code, 
       event_name
from source_data