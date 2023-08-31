{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        spotify_playlist_id,

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
        round((tracks_in_desc / total_tracks) * 100, 1) as percentage_in_desc,

        time(timestamp_seconds(div(cast(youtube_duration as int), 1000))) as youtube_duration,
        time(timestamp_seconds(div(cast(spotify_duration as int), 1000))) as spotify_duration,
        round(cast(difference_ms as int) / 1000, 1) as difference_sec
        

    from {{ ref('int_join_spotify_uris') }}

    order by playlist_name, found, loop_num, found_by
    --order by difference_sec desc, found_by

)

select * from final
