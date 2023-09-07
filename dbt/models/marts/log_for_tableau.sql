{{ config(materialized='view') }}

with

final as (

    select
        log_id,
        spotify_type,
        found_on_try,
        difference_ms,
        tracks_in_desc,
        total_tracks,
        search_type_name,
        status,
        percentage_in_desc,
        difference_sec
    
    from {{ ref('int_join_spotify_uris') }}

)

select * from final
