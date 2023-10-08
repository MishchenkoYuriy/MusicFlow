with

final as (

    select
        found_on_try,
        count(spotify_uri) as records_found

        from {{ ref('int_join_spotify_uris') }}

        group by found_on_try
        order by found_on_try
)

select * from final
