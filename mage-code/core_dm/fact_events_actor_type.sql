{{ config(materialized='table') }}

with dim_actor_type as (
    select type_code, 
           type_name
    from {{ ref('stg_actor_type') }}
),
facts_event as (
    select *
    from {{ ref('stg_events_types') }}
)
select  week,
        SQLDATE,
        source_actor_type.type_name as source_type_name,
        dest_actor_type.type_name as dest_type_name,
        num_events,
        num_mentions,
        num_articles,
        num_sources
from facts_event
inner join dim_actor_type as source_actor_type
on facts_event.source_type = source_actor_type.type_code
inner join dim_actor_type as dest_actor_type
on facts_event.dest_type = dest_actor_type.type_code
where source_actor_type.type_name is not null 
and dest_actor_type.type_name is not null