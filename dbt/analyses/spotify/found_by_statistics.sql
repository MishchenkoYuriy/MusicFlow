with

final as (

    select
        search_type_name as found_by,
        count(spotify_uri) as records_found

        from {{ ref('int_join_spotify_uris') }}

        group by search_type_id, search_type_name
        order by search_type_id
)

select * from final
