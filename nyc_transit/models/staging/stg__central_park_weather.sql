with source as (
  select * from {{ source('main', 'central_park_weather') }}
),
renamed as (

    select
        -- Removing extra whitespace from both ends of string and normalizing case of strings
        UPPER(TRIM(station)) as station,
        UPPER(TRIM(name)) as name,
        date::date as date,
        awnd::double as average_wind_speed,
        prcp::double as precipitation,
        snow::double as snowfall,
        snwd::double as snow_depth,
        tmax::int as maximum_temperature,
        tmin::int as minimum_temperature,
        filename

    from source

)

select * from renamed