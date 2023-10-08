{{ config(materialized='view') }}

with

final as (

    select
        log_id,
        album_uri,
        playlist_uri,
        track_uri,
        found_on_try,
        difference_ms,
        track_match,
        total_tracks,
        q,
        search_type_id,
        status
    
    from {{ source('marts_sources', 'spotify_log') }}
)

select * from final
