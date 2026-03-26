{{ config(materialized='table') }}

with params as (
    select
        -- you can replace this with a dbt var if you need the run date passed in
        current_timestamp as run_ts_utc
),
src0 as (
    select
        fromUser.id                      as author_id
        , unnest(media)                  as media_blob
        , 1                              as seen_count
        , cast(createdAt as timestamp)   as valid_from_ts
        , null                           as valid_to_ts
        , true                           as is_current
        , (select run_ts_utc from params) at time zone 'UTC' at time zone 'US/Eastern' as extract_ts
    from {{ source('duckdb_src', 'stg_chat_messages') }} cm
    where 
        cast(cm.createdAt as timestamp) <
                coalesce(
                    (select min(valid_from_ts) from {{ source('duckdb_src', 'dbt_media_dim') }}),
                    (select run_ts_utc from params) + interval '1' day
                )
         or cast(cm.createdAt as timestamp) >
                coalesce(
                    (select max(valid_from_ts) from {{ source('duckdb_src', 'dbt_media_dim') }}),
                    (select run_ts_utc from params) - interval '99' year
                )
),
src1 as (
    select
          author_id
        , json_extract(media_blob, ['id', 'duration']) as needed_fields
        , media_blob
        , seen_count
        , valid_from_ts
        , valid_to_ts
        , is_current
        , extract_ts
    from src0
)

select distinct
      author_id
    , needed_fields[1]                 as media_id
    , needed_fields[2]                 as media_duration
    , media_blob
    , seen_count
    , valid_from_ts
    , valid_to_ts
    , is_current
    , extract_ts
from src1