{{ config(materialized='ephemeral') }}

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

final as (

    select
        /* spotify_log */
        sl.spotify_uri,
        sl.spotify_playlist_id,
        --sl.youtube_video_id,
        sl.found_on_try,
        sl.difference_ms,
        sl.tracks_in_desc,
        sl.q,
        --sl.search_type_id,
        sl.status,

        /* youtube_videos */
        yv.video_id,
        yv.youtube_title,
        yv.youtube_channel,
        yv.description,
        yv.duration_ms as youtube_duration,

        /* others */
        sp.playlist_name as playlist_name,
        sty.search_type_name,

        /* spotify_albums or spotify_playlists_others or spotify_tracks */
        case
            when sa.album_uri is not null       then 'album'
            when spo.playlist_uri is not null   then 'playlist'
            when st.track_uri is not null       then 'track'
        end as spotify_type,

        coalesce(sa.album_title,    spo.playlist_title,  st.track_title)    as spotify_title, 
        coalesce(sa.album_artists,  spo.playlist_owner,  st.track_artists)  as spotify_artists,
        coalesce(sa.duration_ms,    spo.duration_ms,     st.duration_ms)    as spotify_duration,
        coalesce(sa.total_tracks,   spo.total_tracks,    1)                 as total_tracks
        
    from {{ ref('stg__spotify_log') }} sl
    inner join distinct_videos yv on sl.youtube_video_id = yv.video_id

    inner join {{ ref('stg__spotify_playlists') }} sp on sl.spotify_playlist_id = sp.spotify_playlist_id
    inner join {{ ref('stg__search_types') }} sty on sl.search_type_id = sty.search_type_id

    -- spotify_uri
    left join {{ ref('stg__spotify_albums')}} sa            on sl.spotify_uri = sa.album_uri
    left join {{ ref('stg__spotify_playlists_others')}} spo on sl.spotify_uri = spo.playlist_uri
    left join {{ ref('stg__spotify_tracks' )}} st           on sl.spotify_uri = st.track_uri

)

select * from final
