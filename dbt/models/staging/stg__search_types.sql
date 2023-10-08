{{ config(materialized='view') }}

with

final as (

    select
        search_type_id,
        search_type_name
    
    from {{ source('marts_sources', 'search_types') }}
)

select * from final
