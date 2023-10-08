with

final as (

    select
        spotify_uri,
        spotify_playlist_id,
        user_playlist,

        spotify_title,
        spotify_author,
        --spotify_duration,
        --total_tracks,

        count(video_id) as video_cnt,
        string_agg('https://www.youtube.com/watch?v='||video_id, '\n' order by log_id) as links_to_videos,
        string_agg(cast(log_id as string)||' '||status, '\n' order by log_id) as statuses
    
    from {{ ref('int_join_spotify_uris')}}

    where spotify_playlist_id is not null -- remove other users' playlists

    group by spotify_uri, spotify_playlist_id, user_playlist, spotify_type, spotify_title, spotify_author, spotify_duration, total_tracks
    having count(video_id) > 1

    order by user_playlist, spotify_uri

)

select * from final
