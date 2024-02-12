 -- q1
 create or replace external table `green_taxi_trips.external_green_tt_2022`
 options
(
  format="parquet",
  uris=["gs://taxi_trips_03/green_taxitrips/year=2022/*"]
)

select 
    count(*) 
from 
    `green_taxi_trips.external_green_tt_2022`


-- q2 (data processed by different tables)

create table if not exists `green_taxi_trips.green_tt_2022`
as
select * from `green_taxi_trips.external_green_tt_2022`

select 
    count(distinct PULocationID) 
from `green_taxi_trips.external_green_tt_2022`

select 
    count(distinct PULocationID) 
from `green_taxi_trips.green_tt_2022`

-- q3

select
 count(*)
from
 `green_taxi_trips.green_tt_2022`
where
 fare_amount = 0

 -- q4 (data processed between partitioned and non-partitioned)

  create table if not exists `green_taxi_trips.partitioned_clustered_green_tt_2022`
 partition by date(lpep_pickup_datetime)
 cluster by (pulocationid)
 as
 select * from `green_taxi_trips.green_tt_2022`


select count(distinct PULocationID) from `green_taxi_trips.green_tt_2022`
where lpep_pickup_datetime between '2022-06-01 00:00:00' and  '2022-06-30 23:59:59'

select count(distinct PULocationID) from `green_taxi_trips.partitioned_clustered_green_tt_2022`
where lpep_pickup_datetime between '2022-06-01 00:00:00' and  '2022-06-30 23:59:59'