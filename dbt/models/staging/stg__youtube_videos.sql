{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        youtube_playlist_id,
        title,
        channel_name,
        description,
        duration_ms,
        order_num
    
    from {{ source('marts_sources', 'youtube_videos') }}

)

select * from final
