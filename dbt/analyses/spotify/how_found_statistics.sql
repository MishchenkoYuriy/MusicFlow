with

final as (

    select
        search_type_name as found_by,
        found_on_try as loop_num,
        count(spotify_uri) as records_found

        from {{ ref('int_join_spotify_uris') }}

        group by search_type_id, search_type_name, found_on_try
        order by search_type_id, found_on_try
)

select * from final
