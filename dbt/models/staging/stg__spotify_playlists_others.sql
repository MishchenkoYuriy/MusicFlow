{{ config(materialized='view') }}

with

final as (

    select
        playlist_uri,
        playlist_title,
        playlist_owner,
        duration_ms,
        total_tracks
    
    from {{ source('marts_sources', 'spotify_playlists_others') }}
)

select * from final
