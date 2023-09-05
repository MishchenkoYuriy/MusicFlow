{{ config(materialized='view') }}

with

join_videos_and_playlists as (

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

),

final as (

    select
        s.log_id,
        y.*

    from join_videos_and_playlists y
    left join {{ ref('stg__spotify_log') }} s on y.id = s.log_id
    
    where s.log_id is null

)

select * from final
