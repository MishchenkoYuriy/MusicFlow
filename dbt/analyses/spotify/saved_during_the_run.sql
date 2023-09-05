with

final as (

    select
        spotify_uri,
        user_playlist_id,
        playlist_name,

        spotify_title,
        spotify_artists,
        --spotify_duration,
        --total_tracks,

        count(video_id) as video_cnt,
        string_agg('https://www.youtube.com/watch?v='||video_id, '\n') as links_to_videos,
        string_agg(status, '\n') as statuses
    
    from {{ ref('int_join_spotify_uris')}}

    group by spotify_uri, user_playlist_id, playlist_name, spotify_type, spotify_title, spotify_artists, spotify_duration, total_tracks
    having count(video_id) > 1

    order by playlist_name, spotify_uri

)

select * from final
