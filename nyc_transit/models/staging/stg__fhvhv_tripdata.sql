with source as (
  select * from {{ source('main', 'fhvhv_tripdata') }}
),
renamed as (

    select
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        PULocationID::double as PU_locationID,
        DOLocationID::double as DO_locationID,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf as black_car_fund,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        -- Casting fields to boolean
        CASE
            WHEN UPPER(TRIM(shared_request_flag)) = 'Y' THEN true
            ELSE false
        END as shared_request_flag,
        CASE
            WHEN UPPER(TRIM(shared_match_flag)) = 'Y' THEN true
            ELSE false
        END as shared_match_flag,
        CASE
            WHEN UPPER(TRIM(access_a_ride_flag)) = 'Y' THEN true
            ELSE false
        END as access_a_ride_flag,
        CASE
            WHEN UPPER(TRIM(wav_request_flag)) = 'Y' THEN true
            ELSE false
        END as wav_request_flag,
        CASE
            WHEN UPPER(TRIM(wav_match_flag)) = 'Y' THEN true
            ELSE false
        END as wav_match_flag,
        filename

    from source

    where
        base_passenger_fare >= 0 AND driver_pay >= 0 -- fee should always be positive
)

select * from renamed