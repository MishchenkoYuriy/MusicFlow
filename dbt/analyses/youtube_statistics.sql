with

final as (

    select
        count(video_id) as total_reconds,

        case
        when duration_ms < {{ env_var('DBT_THRESHOLD_MS') }} then 'tracks'
        when duration_ms >= {{ env_var('DBT_THRESHOLD_MS') }} then 'albums/playlists'
        end as video_type,

        case
        when youtube_playlist_id is not null then 'In playlists'
        when youtube_playlist_id is null then 'In liked videos'
        end as location

from {{ ref('stg__youtube_videos') }}

group by video_type, location

)


select * from final
