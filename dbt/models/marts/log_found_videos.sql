{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        user_playlist_id,

        user_playlist,
        spotify_uri,
        spotify_type as found,

        youtube_title,
        youtube_author,
        description,
        spotify_title,
        spotify_author,

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

    order by user_playlist, found, found_on_try, found_by
    --order by difference_sec desc, found_by

)

select * from final
