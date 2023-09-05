with

join_stg__youtube_library as (

    select
        sl.log_id,
        sl.user_playlist_id as spotify_log_spotify_playlist_id,
        yl.youtube_playlist_id
    
    from {{ ref('stg__spotify_log') }} sl
    inner join {{ ref('stg__youtube_library') }} yl on sl.log_id = yl.id

),

join_stg__youtube_playlists as (

    select
        t.log_id,
        t.spotify_log_spotify_playlist_id,
        --t.youtube_playlist_id
        yp.youtube_playlist_id
    
    from join_stg__youtube_library t
    inner join {{ ref('stg__youtube_playlists') }} yp on t.youtube_playlist_id = yp.youtube_playlist_id

),

join_stg__playlist_ids as (

    select
        t.log_id,
        t.spotify_log_spotify_playlist_id,
        --t.youtube_playlist_id,
        --yp.youtube_playlist_id
        p.spotify_playlist_id as playlist_ids_spotify_playlist_id

    from join_stg__youtube_playlists t
    inner join {{ ref('stg__playlist_ids') }} p on t.youtube_playlist_id = p.youtube_playlist_id

),

final as (

    select * from join_stg__playlist_ids
    where spotify_log_spotify_playlist_id != playlist_ids_spotify_playlist_id

)

select * from final
