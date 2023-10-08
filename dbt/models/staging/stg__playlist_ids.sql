{{ config(materialized='view') }}

with

final as (

    select
        id,
        youtube_playlist_id,
        spotify_playlist_id
    
    from {{ source('marts_sources', 'playlist_ids') }}

)

select * from final
