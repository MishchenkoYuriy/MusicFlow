with

final as (

    select
        channel_name,
        count(1) as videos

    from {{ ref('stg__youtube_videos') }}

    group by channel_name

    order by count(1) desc

)

select * from final
