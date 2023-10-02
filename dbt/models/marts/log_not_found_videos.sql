{{ config(materialized='view') }}

with

final as (

    select
        y.*,
        case
        when y.duration_ms < {{ env_var('DBT_THRESHOLD_MS') }} then 'track'
        when y.duration_ms >= {{ env_var('DBT_THRESHOLD_MS') }} then 'album/playlist'
        end as video_type

    from {{ ref('int_useful_youtube_library')}} y
    left join {{ ref('stg__spotify_log') }} s on y.id = s.log_id
    
    where s.log_id is null

)

select * from final
