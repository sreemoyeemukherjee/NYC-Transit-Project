with source as (
  select * from {{ source('main', 'fhv_tripdata') }}
),
renamed as (

    select
        UPPER(TRIM(dispatching_base_num)) as dispatching_base_num,
        pickup_datetime::datetime as pickup_datetime,
        dropOff_datetime::datetime as dropoff_datetime,
        PUlocationID::double as PU_locationID,
        DOlocationID::double as DO_locationID,
        UPPER(TRIM(Affiliated_base_number)) as affiliated_base_number,
        filename

    from source

    where
        dropOff_datetime <= now() -- removing rows with future dates
)

select * from renamed