with source as (
  select * from {{ source('main', 'fhv_bases') }}
),
renamed as (

    select
        UPPER(TRIM(base_number)) as base_number,
        UPPER(TRIM(base_name)) as base_name,
        UPPER(TRIM(dba)) as dba,
        -- Storing categorical values as enum
        UPPER(TRIM(dba_category))::ENUM('OTHER', 'UBER', 'JUNO', 'LYFT', 'VIA') as dba_category ,
        filename

    from source

)

select * from renamed