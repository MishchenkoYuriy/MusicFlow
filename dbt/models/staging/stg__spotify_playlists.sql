{{ config(materialized='view') }}

with

final as (

    select
        spotify_playlist_id,
        playlist_name
    
    from {{ source('marts_sources', 'spotify_playlists') }}
)

select * from final
