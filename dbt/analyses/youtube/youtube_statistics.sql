with

final as (

    select
        count(video_id) as total_reconds,

        case
        when duration_ms < {{ env_var('DBT_THRESHOLD_MS') }} then 'tracks'
        when duration_ms >= {{ env_var('DBT_THRESHOLD_MS') }} then 'albums/playlists'
        end as video_type,

        case
        when youtube_playlist_id = '0' then 'In liked videos'
        else 'In playlists'
        end as section

from {{ ref('int_useful_youtube_library') }}

group by video_type, section

)


select * from final
