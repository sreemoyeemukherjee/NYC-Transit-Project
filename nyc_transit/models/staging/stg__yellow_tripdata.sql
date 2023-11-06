with source as (
  select * from {{ source('main', 'yellow_tripdata') }}
),
renamed as (

    select
        CASE
            WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
            WHEN VendorID = 2 THEN 'VeriFone Inc.'
        END::ENUM('Creative Mobile Technologies, LLC', 'VeriFone Inc.') as vendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        CASE
            WHEN UPPER(TRIM(store_and_fwd_flag)) = 'Y' THEN true
            ELSE false
        END as store_and_fwd_flag,
        CASE
            WHEN RatecodeID = 1 THEN 'Standard rate'
            WHEN RatecodeID = 2 THEN 'JFK'
            WHEN RatecodeID = 3 THEN 'Newark'
            WHEN RatecodeID = 4 THEN 'Nassau or Westchester'
            WHEN RatecodeID = 5 THEN 'Negotiated fare'
            WHEN RatecodeID = 6 THEN 'Group ride'
        END::ENUM('Standard rate','JFK','Newark','Nassau or Westchester','Negotiated fare','Group ride') as RatecodeID,
        PULocationID,
        DOLocationID,
        passenger_count::int as passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        CASE
            WHEN payment_type = 1 THEN 'Credit card'
            WHEN payment_type = 2 THEN 'Cash'
            WHEN payment_type = 3 THEN 'No charge'
            WHEN payment_type = 4 THEN 'Dispute'
            WHEN payment_type = 5 THEN 'Unknown'
            WHEN payment_type = 6 THEN 'Voided trip'
            ELSE 'Unknown'
        END::ENUM('Credit card','Cash','No charge','Dispute','Unknown','Voided trip') as payment_type,
        congestion_surcharge,
        airport_fee,
        filename

    from source

    where
        tpep_pickup_datetime <= now() AND tpep_dropoff_datetime <= now() -- removing rows with future dates
      -- fee should always be positive
      AND trip_distance >= 0 AND fare_amount >= 0 AND extra >= 0 AND mta_tax >=0
      AND tip_amount >= 0 AND improvement_surcharge >= 0 AND total_amount >= 0
      AND airport_fee >= 0
)

select * from renamed