-- Q3 Count of rides in year 2019 for fhv service
with tbl as (
select

  sum(case when extract(date from pickup_datetime) >= '2019-01-01' and extract(date from dropoff_datetime) <= '2019-01-31' then 1 else 0 end) jan_count,
  sum(case when extract(date from pickup_datetime) >= '2019-02-01' and extract(date from dropoff_datetime) <= '2019-02-28' then 1 else 0 end) feb_count,
  sum(case when extract(date from pickup_datetime) >= '2019-03-01' and extract(date from dropoff_datetime) <= '2019-03-31' then 1 else 0 end) mar_count,
  sum(case when extract(date from pickup_datetime) >= '2019-04-01' and extract(date from dropoff_datetime) <= '2019-04-30' then 1 else 0 end) apr_count,
  sum(case when extract(date from pickup_datetime) >= '2019-05-01' and extract(date from dropoff_datetime) <= '2019-05-31' then 1 else 0 end) may_count,
  sum(case when extract(date from pickup_datetime) >= '2019-06-01' and extract(date from dropoff_datetime) <= '2019-06-30' then 1 else 0 end) jun_count,
  sum(case when extract(date from pickup_datetime) >= '2019-07-01' and extract(date from dropoff_datetime) <= '2019-07-31' then 1 else 0 end) jul_count,
  sum(case when extract(date from pickup_datetime) >= '2019-08-01' and extract(date from dropoff_datetime) <= '2019-08-31' then 1 else 0 end) aug_count,
  sum(case when extract(date from pickup_datetime) >= '2019-09-01' and extract(date from dropoff_datetime) <= '2019-09-30' then 1 else 0 end) sep_count,
  sum(case when extract(date from pickup_datetime) >= '2019-10-01' and extract(date from dropoff_datetime) <= '2019-10-31' then 1 else 0 end) oct_count,
  sum(case when extract(date from pickup_datetime) >= '2019-11-01' and extract(date from dropoff_datetime) <= '2019-11-30' then 1 else 0 end) nov_count,
  sum(case when extract(date from pickup_datetime) >= '2019-12-01' and extract(date from dropoff_datetime) <= '2019-12-31' then 1 else 0 end) dec_count
from
  `dbt_taxi_trips.fact_fhv_trips`
where
  extract(year from pickup_datetime) = 2019
), parse_table as (
select
  months,
  cast(counts as numeric) counts
from
  ( select 
      REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(0)], r'^"|"$', '') months, 
      REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(1)], r'^"|"$', '') counts 
    from
      tbl t,
      unnest(SPLIT(REGEXP_REPLACE(to_json_string(t), r'{|}', ''))) pair
    )
)
select sum(counts) from parse_table

-- Q4 Most rides in July 2019 of all services

select * from (
 select
  'green' service_type,
  count(trip_id) ride_count
from
 `dbt_taxi_trips.stg_green_taxi_trips`
where
 pickup_datetime between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
 union all
  select
  'yellow',
  count(trip_id)
from
 `dbt_taxi_trips.stg_yellow_taxi_trips`
where
 pickup_datetime between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
 union all
  select
  'fhv',
  count(Affiliated_base_number)
from
 `dbt_taxi_trips.stg_fhv_taxi_trips`
where
 pickup_datetime between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
)
order by ride_count desc
limit 1

-- Q4 Same question but it uses the fact tables which have a join with zones

select * from (
select
 service_type,
 count(trip_id) num_rides
from
 `dbt_taxi_trips.fact_trips`
 where
   pickup_datetime between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
   group by
   service_type
  union all
select
 'fhv' service_type,
 count(Affiliated_base_number)
from
 `dbt_taxi_trips.fact_fhv_trips`
 where
   pickup_datetime between '2019-07-01 00:00:00' and '2019-07-31 23:59:59'
   group by
   service_type
)
order by num_rides desc
limit 1
