{{ config(materialized='view') }}

with

final as (

    select
        youtube_playlist_id,
        spotify_playlist_id
    
    from {{ source('marts_sources', 'playlists_ids') }}

)

select * from final
