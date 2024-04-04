{{
    config(
        materialized='view'
    )
}}

with agg_events as 
(
    select
        -- identifiers
        week,
        PARSE_DATE("%Y%m%d", Cast(SQLDATE AS String)) as SQLDATE,
        Actor1Type1Code,
        Actor2Type1Code,
        count(1) as num_events,
        sum(NumMentions) as num_mentions,
        sum(NumArticles) as num_articles,
        sum(NumSources) as num_sources
    from {{ env_var('DATASET') }}.events
    where Actor1Type1Code is not null 
    group by week,
             PARSE_DATE("%Y%m%d", Cast(SQLDATE AS String)),
             Actor1Type1Code,
             Actor2Type1Code
)
select  week,
        SQLDATE ,
        Actor1Type1Code as source_type,
        Actor2Type1Code as dest_type,
        num_events,
        num_mentions,
        num_articles,
        num_sources
from agg_events