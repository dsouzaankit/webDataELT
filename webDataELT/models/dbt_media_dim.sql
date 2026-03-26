{{ config(materialized='table') }}

with src as (
    select author_id, media_id, media_duration, media_blob, seen_count, valid_from_ts, extract_ts
    , md5(media_blob) as checksum
    from {{ ref('dbt_src_media_dim') }}
    qualify row_number() over (
        partition by author_id, media_id, md5(media_blob), valid_from_ts
        order by extract_ts
    ) = 1
),
src_ts as (
    select min(valid_from_ts) as min_src_ts
    , max(valid_from_ts) as max_src_ts
    , max(extract_ts) as max_extract_ts
    from src
),
-- Rows in dbt_media_dim that are NOT in this batch: keep as-is
retained as (
    select
          md.author_id
        , md.media_id
        , md.media_duration
        , md.media_blob
        , md.seen_count
        , md.valid_from_ts
        , case when src.media_id is null and md.valid_from_ts 
            between (select min_src_ts from src_ts) 
              and (select max_src_ts from src_ts)
              and md.valid_to_ts is null
            then (select max_extract_ts from src_ts) else md.valid_to_ts end as valid_to_ts
        , case when src.media_id is null and md.valid_from_ts 
            between (select min_src_ts from src_ts) 
              and (select max_src_ts from src_ts)
              and md.is_current
            then false else md.is_current end as is_current
        , case when src.media_id is null and md.valid_from_ts 
            between (select min_src_ts from src_ts) 
              and (select max_src_ts from src_ts)
            then true else false end as is_deleted
        , md.checksum
        , md.extract_ts
    from {{ source('duckdb_src', 'dbt_media_dim') }} md
    left join src
    on md.author_id = src.author_id
    and md.media_id = src.media_id
    and md.checksum = src.checksum
    and md.valid_from_ts = src.valid_from_ts
    where (src.media_id is not null and not md.is_deleted)     -- full duplicate during backfill
                                                               -- deleted reactivation is excluded
    -- checksum is part of unique key for filtering and joins, but not part of the windowing logic
    or not exists (
        select 1 from src
        where md.author_id = src.author_id
          and md.media_id = src.media_id
    )                                                          -- deleted from source during backfill
    qualify row_number() over (
        partition by md.author_id, md.media_id, md.checksum, md.valid_from_ts
        order by md.extract_ts
    ) = 1
),
-- Recalculated history only for (author_id, media_id, valid_from_ts, updated_checksum) in this batch
matching as (
    select
          md.author_id
        , md.media_id
        , md.media_duration
        , md.media_blob
        , md.valid_from_ts
        , md.extract_ts
        , false as is_deleted
        , md.checksum
    from {{ source('duckdb_src', 'dbt_media_dim') }} md
    where not exists (
        select 1 from retained uchg
        where md.author_id = uchg.author_id
          and md.media_id = uchg.media_id
          and md.checksum = uchg.checksum
          and md.valid_from_ts = uchg.valid_from_ts
    )
    qualify row_number() over (
        partition by md.author_id, md.media_id, md.checksum, md.valid_from_ts
        order by md.extract_ts
    ) = 1
),
new_batch as (
    select
          author_id
        , media_id
        , media_duration
        , media_blob
        , valid_from_ts
        , extract_ts
        , false as is_deleted
        , checksum
    from src
    where not exists (
        select 1 from retained uchg
        where src.author_id = uchg.author_id
          and src.media_id = uchg.media_id
          and src.checksum = uchg.checksum
          and src.valid_from_ts = uchg.valid_from_ts
    )
),
combined_data as (
    select * from matching
    union all by name
    select * from new_batch
),
windowed_logic as (
    select
          author_id
        , media_id
        , media_duration
        , media_blob
        , row_number() over (
              partition by author_id, media_id
              order by valid_from_ts, extract_ts
          )                                   as seen_count
        , valid_from_ts
        , lead(valid_from_ts) over (
              partition by author_id, media_id
              order by valid_from_ts, extract_ts
          )                                   as valid_to_ts
        , extract_ts
        , max(extract_ts) over (
              partition by author_id, media_id, valid_from_ts
              rows between unbounded preceding and unbounded following
          )                                   as max_extract_ts
        , is_deleted
        , cd.checksum
    from combined_data cd
),
recalculated as (
    select
          author_id
        , media_id
        , media_duration
        , media_blob
        , seen_count
        , valid_from_ts
        , valid_to_ts
        , max_extract_ts as extract_ts
        , is_deleted
        , case when valid_to_ts is null and not is_deleted then true else false end as is_current
        , wl.checksum
    from windowed_logic wl
    qualify row_number() over (
        partition by author_id, media_id, wl.checksum, valid_from_ts
        order by extract_ts
    ) = 1
)

select * from retained
union all by name
select * from recalculated
