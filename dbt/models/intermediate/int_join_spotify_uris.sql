{{ config(materialized='ephemeral') }}

with

join_library_with_log as (

    select
        sl.*,
        yl.youtube_playlist_id,
        yl.video_id
    
    from {{ ref('stg__spotify_log') }} sl
    inner join {{ ref('stg__youtube_library') }} yl on sl.log_id = yl.id

),

join_playlist_info as (

    select
        l.*,

        yp.type,
        yp.title,
        yp.author,
        yp.year,

        p.spotify_playlist_id
    
    from join_library_with_log l
    inner join {{ ref('stg__youtube_playlists') }} yp on l.youtube_playlist_id = yp.youtube_playlist_id
    left join {{ ref('stg__playlist_ids')}} p on yp.youtube_playlist_id = p.youtube_playlist_id

),

join_uris as (

    select
        /* spotify_log */
        p.log_id,
        p.youtube_playlist_id,
        p.spotify_playlist_id,
        p.found_on_try,
        p.difference_ms,
        p.track_match,
        p.total_tracks,
        p.q,
        p.search_type_id,
        p.status,

        /* youtube_playlists */
        p.type,
        p.title,
        p.author,
        p.year,

        /* youtube_videos */
        yv.video_id,
        yv.type as video_type,
        yv.title as video_title,
        yv.author as video_author,
        yv.description,
        yv.duration_ms as video_duration,

        /* others */
        sp.title as user_playlist,
        sty.search_type_name,

        /* spotify_albums or spotify_playlists_others or spotify_tracks */
        case
            when p.album_uri is not null       then 'Album'
            when p.playlist_uri is not null   then 'Playlist'
            when p.track_uri is not null       then 'Track'
        end as spotify_type,

        coalesce(p.album_uri,      p.playlist_uri,     p.track_uri)      as spotify_uri,
        coalesce(sa.album_title,    spo.playlist_title,  st.track_title)    as spotify_title,
        coalesce(sa.album_artists,  spo.playlist_owner,  st.track_artists)  as spotify_author,
        coalesce(sa.duration_ms,    spo.duration_ms,     st.duration_ms)    as spotify_duration
        
    from join_playlist_info p
    inner join {{ ref('stg__youtube_videos') }} yv on p.video_id = yv.video_id

    left join {{ ref('stg__spotify_playlists') }} sp on p.spotify_playlist_id = sp.spotify_playlist_id
    inner join {{ ref('stg__search_types') }} sty on p.search_type_id = sty.search_type_id

    -- spotify_uri
    left join {{ ref('stg__spotify_albums')}} sa            on p.album_uri = sa.album_uri
    left join {{ ref('stg__spotify_playlists_others')}} spo on p.playlist_uri = spo.playlist_uri
    left join {{ ref('stg__spotify_tracks' )}} st           on p.track_uri = st.track_uri

),

final as (

    select
        log_id,
        youtube_playlist_id,
        spotify_playlist_id,
        user_playlist,
        found_on_try,
        difference_ms,
        q,
        search_type_id,
        status,

        type,
        title,
        author,
        year,

        video_id,
        video_type,
        video_title,
        video_author,
        description,
        video_duration,

        search_type_name,

        spotify_type,
        spotify_uri,
        spotify_title,
        spotify_author,
        spotify_duration,
        track_match,
        total_tracks,

        round((track_match / total_tracks) * 100, 1) as percentage_in_desc,

        time(timestamp_seconds(div(video_duration, 1000))) as youtube_duration_timestamp,
        time(timestamp_seconds(div(spotify_duration, 1000))) as spotify_duration_timestamp,
        round(difference_ms / 1000, 1) as difference_sec

    from join_uris

)

select * from final
