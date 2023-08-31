with

final as (

    select
        youtube_channel,
        count(1) as videos

    from {{ ref('stg__youtube_videos') }}

    group by youtube_channel

    order by count(1) desc

)

select * from final
