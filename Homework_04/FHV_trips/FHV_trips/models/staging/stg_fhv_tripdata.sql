with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        dispatching_base_num as dispatching_base_number,
        pickup_datetime,
        dropoff_datetime,
        pu_locationid as pickup_location_id,
        do_locationid as dropoff_location_id,
        sr_flag as sr_flag,
        affiliated_base_num as affiliated_base_number
    from source
)

select * from renamed
