with

final as (

    select
        author as youtube_channel,
        count(1) as videos

    from {{ ref('stg__youtube_videos') }}

    group by author

    order by count(1) desc

)

select * from final
