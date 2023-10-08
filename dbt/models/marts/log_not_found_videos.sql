{{ config(materialized='view') }}

with

final as (

    select
        y.*

    from {{ ref('int_useful_youtube_library')}} y
    left join {{ ref('stg__spotify_log') }} s on y.id = s.log_id
    
    where s.log_id is null

)

select * from final
