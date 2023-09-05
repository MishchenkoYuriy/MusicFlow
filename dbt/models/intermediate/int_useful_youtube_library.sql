{{ config(materialized='ephemeral') }}


with

final as (

    select
        yl.id,

        yp.youtube_playlist_id,
        yp.playlist_name,

        yv.video_id,
        yv.youtube_title,
        yv.youtube_channel,
        yv.description,
        yv.duration_ms,
    
    from {{ ref('stg__youtube_library') }} yl
    inner join {{ ref('stg__youtube_playlists') }} yp on yl.youtube_playlist_id = yp.youtube_playlist_id
    inner join {{ ref('stg__youtube_videos') }} yv on yl.video_id = yv.video_id

)

select * from final
