with flights as (

    select * from {{ ref('slv_flights') }}

),

hourly as (

    select
        departed_at::date                           as flight_date,
        hour(departed_at)                           as departure_hour_utc,
        queried_airport                             as airport_icao,
        flight_direction,
        count(*)                                    as flight_count,
        avg(flight_duration_minutes)                as avg_duration_min
    from flights
    group by 1, 2, 3, 4

)

select
    h.*,
    ap.airport_name,
    ap.city,
    ap.country
from hourly h
left join {{ ref('slv_airports') }} ap
    on h.airport_icao = ap.airport_icao
