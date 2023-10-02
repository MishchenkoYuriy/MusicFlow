{{ config(materialized='view') }}

with

join_videos as (

    select
        yl.id,
        case
        when yv.duration_ms < {{ env_var('DBT_THRESHOLD_MS') }} then 'track'
        when yv.duration_ms >= {{ env_var('DBT_THRESHOLD_MS') }} then 'album/playlist'
        end as video_type
    
    from {{ ref('stg__youtube_library') }} yl
    inner join {{ ref('stg__youtube_videos') }} yv on yl.video_id = yv.video_id

),

join_spotify_log as (

    select
        v.id as log_id,
        v.video_type,

        s.album_uri,
        s.playlist_uri,
        case
            when s.album_uri is not null       then 'album'
            when s.playlist_uri is not null   then 'playlist'
            when s.track_uri is not null       then 'track'
        end as spotify_type,

        s.found_on_try,
        s.difference_ms,
        s.track_match,
        s.search_type_id,
        s.status

    from join_videos v
    left join {{ ref('stg__spotify_log') }} s on v.id = s.log_id

),

join_total_tracks as (

    select
        sl.log_id,
        sl.video_type,
        sl.spotify_type,
        sl.found_on_try,
        sl.difference_ms,
        sl.track_match,
        sl.search_type_id,
        sl.status,
        coalesce(sa.total_tracks, spo.total_tracks, 1) as total_tracks, --TODO
        round(sl.difference_ms / 1000, 1) as difference_sec
    
    from join_spotify_log sl
    left join {{ ref('stg__spotify_albums')}} sa            on sl.album_uri = sa.album_uri
    left join {{ ref('stg__spotify_playlists_others')}} spo on sl.playlist_uri = spo.playlist_uri

),

final as (

    select
        log_id,
        video_type,
        spotify_type,
        found_on_try,
        difference_ms,
        track_match,
        total_tracks,
        search_type_id,
        status,
        round((track_match / total_tracks) * 100, 1) as percentage_in_desc,
        difference_sec
    
    from join_total_tracks

)

select * from final
