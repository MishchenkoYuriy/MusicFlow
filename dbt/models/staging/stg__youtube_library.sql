{{ config(materialized='view') }}

with

final as (

    select
        id,
        youtube_playlist_id,
        video_id
    
    from {{ source('marts_sources', 'youtube_library') }}
)

select * from final
