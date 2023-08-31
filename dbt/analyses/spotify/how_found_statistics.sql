with

template as (

    select
        sty.search_type_id,
        sty.search_type_name as found_by,
        sl.found_on_try as loop_num,
        sl.spotify_uri

        from {{ ref('stg__spotify_log') }} sl
        inner join {{ ref('stg__search_types') }} sty on sl.search_type_id = sty.search_type_id

),

final as (

    select
        found_by,
        loop_num,
        count(spotify_uri) as records_found

        from template

        group by search_type_id, found_by, loop_num
        order by search_type_id, loop_num
)

select * from final
