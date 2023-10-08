{{ config(materialized='view') }}

with

final as (

    select
        album_uri,
        album_title,
        album_artists,
        duration_ms,
        total_tracks
    
    from {{ source('marts_sources', 'spotify_albums') }}
)

select * from final
