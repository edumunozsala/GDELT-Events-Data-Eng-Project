{{
    config(
        materialized='view'
    )
}}

with agg_events_by_code as 
(
    select
        -- identifiers
        week,
        SQLDATE ,
        EventRootCode,
        EventBaseCode,
        EventCode,
        count(1) as num_events,
        sum(NumMentions) as num_mentions,
        sum(NumArticles) as num_articles,
        sum(NumSources) as num_sources
    from {{ env_var('DATASET') }}.events
    where EventRootCode is not null 
    group by week,
            SQLDATE,
            EventRootCode,
            EventBaseCode,
            EventCode
)
select  week,
        SQLDATE,
        EventRootCode,
        EventBaseCode,
        EventCode,
        num_events,
        num_mentions,
        num_articles,
        num_sources
from agg_events_by_code