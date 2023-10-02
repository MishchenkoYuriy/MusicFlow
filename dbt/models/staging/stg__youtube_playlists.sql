{{ config(materialized='view') }}

with

final as (

    select
        youtube_playlist_id,
        type,
        title,
        author,
        year
    
    from {{ source('marts_sources', 'youtube_playlists') }}

)

select * from final
