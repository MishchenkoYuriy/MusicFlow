{{ config(materialized='view') }}

with

final as (

    select
        track_uri,
        album_uri,
        playlist_uri,
        track_title,
        track_artists,
        duration_ms
    
    from {{ source('marts_sources', 'spotify_tracks') }}
)

select * from final
