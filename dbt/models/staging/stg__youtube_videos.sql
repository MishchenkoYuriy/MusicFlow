{{ config(materialized='view') }}

with

final as (

    select
        video_id,
        type,
        title,
        author,
        description,
        duration_ms
    
    from {{ source('marts_sources', 'youtube_videos') }}

)

select * from final
