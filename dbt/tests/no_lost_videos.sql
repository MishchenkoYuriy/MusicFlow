with

total as (
    
    select count(1) as total_cnt
    from {{ ref('stg__youtube_library') }}

),

found as (

    select count(1) as found_cnt
    from {{ ref('int_join_spotify_uris')}}

),

not_found as (

    select count(1) as not_found_cnt
    from {{ ref('log_not_found_videos') }}

),

final as (

    select 1

    from total, found, not_found

    where total_cnt != found_cnt + not_found_cnt

)

select * from final
