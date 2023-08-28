{{ config(materialized='view') }}

with

final as (

    select
        youtube_playlist_id,
        playlist_name
    
    from {{ source('marts_sources', 'youtube_playlists') }}

)

select * from final
