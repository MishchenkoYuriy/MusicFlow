{% test duration_match(model, column_name) %}

with

join_and_calculate as (

    select
        m.{{ column_name }} as spotify_uri,
        m.duration_ms as model_duration,
        sum(st.duration_ms) as tracks_duration
    
    from {{ model }} m

    inner join {{ ref('stg__spotify_tracks') }} st on m.{{ column_name }} = st.{{ column_name }}

    group by m.{{ column_name }}, model_duration

),

final as (

    select
        spotify_uri,
        model_duration,
        tracks_duration
    
    from join_and_calculate

    where model_duration != tracks_duration

)

select * from final

{% endtest %}
