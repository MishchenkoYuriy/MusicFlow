with

template as (

    select
        yv.video_id,
        yv.youtube_title,
        yv.youtube_channel,
        coalesce(yp.playlist_name, 'Liked videos') as playlist_name
    
    from {{ ref('stg__youtube_videos') }} yv
    left join {{ ref('stg__youtube_playlists') }} yp on yv.youtube_playlist_id = yp.youtube_playlist_id

    order by yv.youtube_playlist_id

),

final as (

    select
        youtube_title,
        youtube_channel,
        'https://www.youtube.com/watch?v='||video_id link,
        count(1) as section_cnt,
        string_agg(playlist_name, '; ') as sections
    
    from template

    group by video_id, youtube_title, youtube_channel
    having count(1) > 1

    order by section_cnt desc

)

select * from final
