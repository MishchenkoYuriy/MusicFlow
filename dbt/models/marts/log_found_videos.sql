{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        user_playlist_id,

        playlist_name,
        spotify_uri,
        spotify_type as found,

        youtube_title,
        youtube_channel,
        description,
        spotify_title,
        spotify_artists,

        q,
        search_type_name as found_by,
        found_on_try as loop_num,
        status,

        tracks_in_desc,
        total_tracks,
        percentage_in_desc,

        youtube_duration_timestamp,
        spotify_duration_timestamp,
        difference_sec

    from {{ ref('int_join_spotify_uris') }}

    order by playlist_name, found, loop_num, found_by
    --order by difference_sec desc, found_by

)

select * from final
