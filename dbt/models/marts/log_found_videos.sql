{{ config(materialized='view') }}

with

current_users_music as (

    select
        video_id,
        spotify_playlist_id,
        user_playlist,

        youtube_playlist_id,
        spotify_uri,
        spotify_type as found,

        video_title as youtube_title,
        spotify_title,

        video_title as youtube_author,
        spotify_author,

        description,

        q,
        search_type_name as found_by,
        found_on_try,
        status,

        track_match,
        total_tracks,
        percentage_in_desc,

        youtube_duration_timestamp,
        spotify_duration_timestamp,
        difference_sec

    from {{ ref('int_join_spotify_uris') }}
    where spotify_playlist_id is not null

),

other_users_music as (

    select
        cast(null as string) video_id,
        spotify_playlist_id,
        user_playlist,

        youtube_playlist_id,
        spotify_uri,
        spotify_type as found,

        title as youtube_title,
        spotify_title,

        string_agg(DISTINCT video_author, '; ') as youtube_author,
        spotify_author,

        cast(null as string) description,

        q,
        search_type_name as found_by,
        found_on_try,
        status,

        track_match,
        total_tracks,
        percentage_in_desc,

        time(timestamp_seconds(div(sum(video_duration), 1000))) as youtube_duration_timestamp,
        spotify_duration_timestamp,
        difference_sec

    from {{ ref('int_join_spotify_uris') }}
    where spotify_playlist_id is null

    group by
        youtube_playlist_id,
        spotify_playlist_id,

        user_playlist,
        spotify_uri,
        spotify_type,

        title,
        spotify_title,
        spotify_author,

        q,
        search_type_name,
        found_on_try,
        status,

        track_match,
        total_tracks,
        percentage_in_desc,

        spotify_duration_timestamp,
        difference_sec
    
),

final as (

    select * from current_users_music
    union all
    select * from other_users_music

)

select * from final
