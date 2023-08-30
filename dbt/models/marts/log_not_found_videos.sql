{{ config(materialized='view') }}

with

join_playlist_ids as (

    select
        yv.video_id,
        yv.youtube_playlist_id,
        p.spotify_playlist_id,

        coalesce(yp.playlist_name, 'Liked videos') as playlist_name,
        yv.title,
        yv.channel_name,
        yv.duration_ms
    
    from {{ ref('stg__youtube_videos') }} yv
    -- left joins because of nulls in youtube_playlist_id ('Liked videos') 
    left join {{ ref('stg__youtube_playlists') }} yp on yv.youtube_playlist_id = yp.youtube_playlist_id
    left join {{ ref('stg__playlists_ids') }} p on yv.youtube_playlist_id = p.youtube_playlist_id

),

final as (

    select
        yv.video_id,
        yv.youtube_playlist_id,

        yv.playlist_name,
        yv.title,
        yv.channel_name,
        yv.duration_ms
    
    from join_playlist_ids yv

    left join {{ ref('stg__spotify_log') }} sl
    on yv.video_id = sl.youtube_video_id
    and (yv.spotify_playlist_id = sl.spotify_playlist_id  or (yv.spotify_playlist_id is null and sl.spotify_playlist_id is null))

    where sl.spotify_uri is null -- not found

    order by yv.youtube_playlist_id, yv.title

)

select * from final
