with airports_seed as (

    select * from {{ ref('seed_airports') }}

)

select
    airport_icao,
    airport_name,
    city,
    country,
    latitude,
    longitude
from airports_seed
