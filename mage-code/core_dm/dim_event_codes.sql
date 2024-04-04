{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ ref('stg_event_codes') }}
)
select event_code, 
       event_name
from source_data