with

final as (

    select
        count(video_id) as total_reconds,
        estimated_type,

        case
        when youtube_playlist_id = 'LM' then 'In liked videos'
        else 'In playlists'
        end as section

from {{ ref('int_useful_youtube_library') }}

group by estimated_type, section

)

select * from final
