{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        youtube_title,
        youtube_channel,
        description,
        duration_ms
    
    from {{ source('marts_sources', 'youtube_videos') }}

)

select * from final
