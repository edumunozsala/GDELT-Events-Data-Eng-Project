{{ config(materialized='view') }}

with source_data as (
    select code as type_code, 
           label as type_name
    from {{ env_var('DATASET') }}.lookups
    where table='type'
)
select type_code, 
       type_name
from source_data