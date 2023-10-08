with

final as (

    select
        yp.youtube_playlist_id,
        yp.type,
        yp.title,
        yp.author,
        count(sl.log_id) as found_tracks,
        count(yl.id) as total_tracks,
        round(count(sl.log_id) * 100 / count(yl.id), 2)  as percentage_found
    
    from {{ ref('stg__youtube_playlists') }} yp
    inner join {{ ref('stg__youtube_library') }} yl on yp.youtube_playlist_id = yl.youtube_playlist_id
    left join {{ ref('stg__spotify_log' )}} sl on yl.id = sl.log_id

    group by yp.youtube_playlist_id, yp.type, yp.title, yp.author
    order by percentage_found

)

select * from final
