with

final as (

    select
        title,
        author,
        'https://www.youtube.com/watch?v='||video_id link,
        count(1) as section_cnt,
        string_agg(playlist_name, '; ') as sections
    
    from {{ ref('int_useful_youtube_library') }}

    group by video_id, title, author
    having count(1) > 1

    order by section_cnt desc

)

select * from final
