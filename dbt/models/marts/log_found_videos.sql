{{ config(materialized='view') }}

with

distinct_videos as (

    select
        distinct video_id,
        youtube_title,
        youtube_channel,
        description,
        duration_ms
    
    from {{ ref('stg__youtube_videos') }}
),

join_spotify_uris as (

    select
        yv.video_id,
        sl.spotify_playlist_id,

        coalesce(sp.playlist_name, 'Liked videos') as playlist_name,
        sl.spotify_uri,
        case
            when sa.album_uri is not null then 'album'
            when spo.playlist_uri is not null then 'playlist'
            when st.track_uri is not null then 'track'
        end as found,

        yv.youtube_title,
        yv.youtube_channel,
        yv.description,

        coalesce(sa.album_title, spo.playlist_title, st.track_title) as spotify_title, 
        coalesce(sa.album_artists, spo.playlist_owner, st.track_artists) as spotify_artists,

        sl.q,
        sty.search_type_name as found_by,
        sl.found_on_try as loop_num,
        sl.status,

        sl.tracks_in_desc,
        coalesce(sa.total_tracks, spo.total_tracks, 1) as total_tracks,

        yv.duration_ms youtube_duration,
        coalesce(sa.duration_ms, spo.duration_ms, st.duration_ms) spotify_duration,
        sl.difference_ms
        
    from {{ ref('stg__spotify_log') }} sl

    left join {{ ref('stg__spotify_playlists') }} sp on sl.spotify_playlist_id = sp.spotify_playlist_id
    inner join distinct_videos yv on sl.youtube_video_id = yv.video_id
    inner join {{ ref('stg__search_types') }} sty on sl.search_type_id = sty.search_type_id

    -- spotify_uri
    left join {{ ref('stg__spotify_albums')}} sa            on sl.spotify_uri = sa.album_uri
    left join {{ ref('stg__spotify_playlists_others')}} spo on sl.spotify_uri = spo.playlist_uri
    left join {{ ref('stg__spotify_tracks' )}} st           on sl.spotify_uri = st.track_uri

),

final as (

    select
        video_id,
        spotify_playlist_id,

        playlist_name,
        spotify_uri,
        found,

        youtube_title,
        youtube_channel,
        description,
        spotify_title,
        spotify_artists,

        q,
        found_by,
        loop_num,
        status,

        tracks_in_desc,
        total_tracks,
        round((tracks_in_desc / total_tracks) * 100, 1) as percentage_in_desc,

        time(timestamp_seconds(div(cast(youtube_duration as int), 1000))) as youtube_duration,
        time(timestamp_seconds(div(cast(spotify_duration as int), 1000))) as spotify_duration,
        round(cast(difference_ms as int) / 1000, 1) as difference_sec
        

    from join_spotify_uris

    order by playlist_name, found, loop_num, found_by
    --order by difference_sec desc, found_by

)

select * from final
