{{ config(materialized='table') }}

with dim_event_code as (
    select event_code, 
           event_name
    from {{ ref('stg_event_codes') }}
),
facts_event as (
    select *
    from {{ ref('stg_events_by_code') }}
)
select  week,
        SQLDATE ,
        root_event_code.event_name as root_event_name,
        base_event_code.event_name as base_event_name,
        event_code.event_name as event_name,
        num_events,
        num_mentions,
        num_articles,
        num_sources
from facts_event
inner join dim_event_code as root_event_code
on facts_event.EventRootCode = root_event_code.event_code
inner join dim_event_code as base_event_code
on facts_event.EventBaseCode = base_event_code.event_code
inner join dim_event_code as event_code
on facts_event.EventCode = event_code.event_code
where facts_event.EventRootCode is not null
