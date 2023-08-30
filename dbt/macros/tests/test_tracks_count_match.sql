{% test tracks_count_match(model, column_name) %}

with

join_and_calculate as (

    select
        m.{{ column_name }} as uri,
        m.total_tracks as model_tracks_cnt,
        count(st.track_uri) tracks_cnt
    
    from {{ model }} m

    inner join {{ ref('stg__spotify_tracks') }} st on m.{{ column_name }} = st.{{ column_name }}

    group by m.{{ column_name }}, model_tracks_cnt

),

final as (

    select
        uri,
        model_tracks_cnt,
        tracks_cnt
    
    from join_and_calculate

    where model_tracks_cnt != tracks_cnt

)

select * from final

{% endtest %}
