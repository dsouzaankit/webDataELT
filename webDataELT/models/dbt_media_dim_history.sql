{{ config(materialized='table') }}

select *
from {{ ref('dbt_media_dim') }}