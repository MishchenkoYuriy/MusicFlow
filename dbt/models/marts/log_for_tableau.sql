{{ config(materialized='view') }}


with

current_users_music as (

    select
        s.log_id,
        yv.video_id,
        case
            when yv.duration_ms < {{ env_var('DBT_THRESHOLD_MS') }} then 'Track'
            when yv.duration_ms >= {{ env_var('DBT_THRESHOLD_MS') }} then 'Album/Playlist'
        end as youtube_type,
        yv.type as music_type,

        case
            when s.album_uri is not null      then 'Album'
            when s.playlist_uri is not null   then 'Playlist'
            when s.track_uri is not null      then 'Track'
        end as spotify_type,

        s.found_on_try,
        s.search_type_id,
        s.difference_ms,
        round(s.difference_ms / 1000, 1) as difference_sec,
        round(s.difference_ms / 60000, 2) as difference_m,
        time(timestamp_seconds(div(s.difference_ms, 1000))) as difference_timestamp,
        s.track_match,
        s.total_tracks,
        round((s.track_match / s.total_tracks) * 100, 1) as percentage_in_desc,
    
    from {{ ref('stg__youtube_library') }} yl
    inner join {{ ref('stg__youtube_playlists') }} yp on yl.youtube_playlist_id = yp.youtube_playlist_id
    inner join {{ ref('stg__youtube_videos') }} yv on yl.video_id = yv.video_id
    left join {{ ref('stg__spotify_log') }} s on yl.id = s.log_id

    where yp.author = "{{ env_var('DBT_YOUR_CHANNEL_NAME') }}" or yp.author is null

),

other_users_music as (

    select
        null as log_id,
        cast(null as string) as video_id,
        yp.type as youtube_type,
        cast(null as string) as music_type,

        case
            when s.album_uri is not null      then 'Album'
            when s.playlist_uri is not null   then 'Playlist'
            when s.track_uri is not null      then 'Track'
        end as spotify_type,

        s.found_on_try,
        s.search_type_id,
        s.difference_ms,
        round(s.difference_ms / 1000, 1) as difference_sec,
        round(s.difference_ms / 60000, 2) as difference_m,
        time(timestamp_seconds(div(s.difference_ms, 1000))) as difference_timestamp,
        s.track_match,
        s.total_tracks,
        round((s.track_match / s.total_tracks) * 100, 1) as percentage_in_desc,

    from {{ ref('stg__youtube_library') }} yl
    inner join {{ ref('stg__youtube_playlists') }} yp on yl.youtube_playlist_id = yp.youtube_playlist_id
    inner join {{ ref('stg__youtube_videos') }} yv on yl.video_id = yv.video_id
    left join {{ ref('stg__spotify_log') }} s on yl.id = s.log_id

    where yp.author != "{{ env_var('DBT_YOUR_CHANNEL_NAME') }}" and yp.author is not null

    group by
        yp.youtube_playlist_id,
        yp.type,
        s.album_uri,
        s.playlist_uri,
        s.track_uri,
        s.found_on_try,
        s.search_type_id,
        s.difference_ms,
        s.track_match,
        s.total_tracks

),

youtube_grouped_library as (

    select * from current_users_music
    union all
    select * from other_users_music

),

final as (

    select
        row_number() over (order by search_type_id) as id,
        log_id,
        video_id,
        youtube_type,
        music_type,
        spotify_type,
        found_on_try,
        search_type_id,
        difference_ms,
        case
            when difference_sec = 0 then 0.1 --fix for logarithmic scale axis
            else difference_sec
        end as difference_sec,
        difference_m,
        difference_timestamp,
        track_match,
        total_tracks,
        percentage_in_desc
    
    from youtube_grouped_library
)

select * from final
