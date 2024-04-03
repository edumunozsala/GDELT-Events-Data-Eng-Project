{{ config(materialized='table') }}

with source_data as (
    select *
    from {{ ref('stg_actor_type') }}
)
select type_code, 
       type_name
from source_data