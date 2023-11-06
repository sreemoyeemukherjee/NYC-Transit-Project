with source as (
  select * from {{ source('main', 'bike_data') }}
),
renamed as (
    select
        -- Replacing null with appropriate value
        COALESCE(tripduration::int, 0) as trip_duration_in_seconds,
        -- Combining different data formats
        COALESCE(starttime::datetime, started_at::datetime) as start_time,
        COALESCE(stoptime::datetime, ended_at::datetime) as stop_time,
        COALESCE("start station id"::int, start_station_id::int) as start_station_id,
        COALESCE(UPPER(TRIM("start station name")), UPPER(TRIM(start_station_name))) as start_station_name,
        COALESCE("start station latitude"::double, start_lat::double) as start_station_latitude,
        COALESCE("start station longitude"::double, start_lng::double) as start_station_longitude,
        COALESCE("end station id"::int, end_station_id::int) as end_station_id,
        COALESCE(UPPER(TRIM("end station name")), UPPER(TRIM(end_station_name))) as end_station_name,
        COALESCE("end station latitude"::double, end_lat::double) as end_station_latitude,
        COALESCE("end station longitude"::double, end_lng::double) as end_station_longitude,
        bikeid::int as bike_id,
        TRIM(usertype)::ENUM ('Customer', 'Subscriber') as user_type,
        "birth year"::int as birth_year,
        CASE
            WHEN TRIM(gender) = '0' THEN 'Unknown'
            WHEN TRIM(gender) = '1' THEN 'Male'
            WHEN TRIM(gender) = '2' THEN 'Female'
            ELSE 'Unknown' -- default case
        END as gender,
        ride_id,
        TRIM(rideable_type)::ENUM('classic_bike', 'docked_bike', 'electric_bike') as rideable_type,
        TRIM(member_casual)::ENUM('member', 'casual') as member_casual,
        filename

    from source

)

select * from renamed