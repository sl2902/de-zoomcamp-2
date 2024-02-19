{{ 
    config(materialized='table') 
}}


select
  Affiliated_base_number,
  cast(PUlocationID as integer) as pickup_locationid,
  cast(DOlocationID as integer) as dropoff_locationid,
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropOff_datetime as timestamp) as dropoff_datetime,
  SR_Flag,
  dispatching_base_num
from 
    {{ source('staging', 'stg_fhv_taxi_trips') }}
-- where Affiliated_base_number is not null

-- dbt build -m stg_fhv_taxi_trips.sql --vars 'is_test_run: false' --target=dev
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}