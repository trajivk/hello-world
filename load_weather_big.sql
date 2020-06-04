/*--------------------------------------------------------------------------------
  LOAD WEATHER V2

  #2 in the core demo flow when you are running on the 1Bn record demo.
  Run this in your demo account.

  This script loads the WEATHER table from staged JSON files. It shows how
  Snowflake can handle semi-structured data with similar performance to structured
  data. It also shows cloning as a way of supporting agile devops. It also shows
  how Snowflake supports "real world" SQL with CTEs and UDFs.

  Author:   Alan Eldridge
  Updated:  10 June 2019

  #weather #loading #semistructured #json #vertical #scalability #elasticity
  #cloning #dev #devops #cte #udf #flatten #big #billion
--------------------------------------------------------------------------------*/

use role dba_citibike;

/*--------------------------------------------------------------------------------
  Jane asks for more data to use in her analysis - weather data

  But John is a good DBA and his response is... NO DEV/TEST IN PROD!!!!!

  So we need a DEV environment...
--------------------------------------------------------------------------------*/

-- make a copy of our current state PROD database to use as DEV
create or replace database citibike_dev clone citibike;

-- also make a DEV warehouse so our DEV queries are isolated from PROD
create warehouse if not exists dev_wh warehouse_size = 'large' auto_suspend = 300 initially_suspended=true;


/*--------------------------------------------------------------------------------
  We have staged the weather data, but it's in JSON format
--------------------------------------------------------------------------------*/

-- create the stage
create or replace stage weather
  url = 's3://citibike-demo-us-west/V2/big/weather'
  credentials = (aws_key_id = 'AKIAILDSC3QV7KIM3MNQ' aws_secret_key = 'gVB0McQsohfQbhQoBxM8vaOA1pHBZx1mKlgnu6va')
  file_format = json;

list @weather/;

select $1 from @weather/2019/ limit 20;


/*--------------------------------------------------------------------------------
  Load the weather data into a VARIANT data type
--------------------------------------------------------------------------------*/

create or replace table weather (v variant, t timestamp);

alter warehouse dev_wh set warehouse_size='3x-large';

-- load the data and convert the timezone from UTC to US/Eastern to match trip data
copy into weather from
  (select $1, convert_timezone('UTC', 'US/Eastern', $1:time::timestamp_ntz)
  from @weather/);

alter warehouse dev_wh set warehouse_size='large';

select count(*) from weather;

select * from weather limit 20;


/*--------------------------------------------------------------------------------
  We can reference the JSON data as if it were structured
--------------------------------------------------------------------------------*/

select v, t, v:city.name::string city, v:weather[0].main::string conditions from weather
  where v:city.name = 'New York' and v:weather[0].main = 'Snow'
  limit 100;

-- and we can unwrap complex structures such as arrays via FLATTEN
-- to compare the most common weather in different cities
select value:main::string as conditions
  ,sum(iff(v:city.name::string='New York',1,0)) as nyc_freq
  ,sum(iff(v:city.name::string='Seattle',1,0)) as seattle_freq
  ,sum(iff(v:city.name::string='San Francisco',1,0)) as san_fran_freq
  ,sum(iff(v:city.name::string='Miami',1,0)) as miami_freq
  ,sum(iff(v:city.name::string='Washington, D. C.',1,0)) as wash_freq
  from weather w,
  lateral flatten (input => w.v:weather) wf
  where v:city.name in ('New York','Seattle','San Francisco','Miami','Washington, D. C.')
    and year(t) = 2019
  group by 1;


/*--------------------------------------------------------------------------------
  We have tested the weather data and it's good.
  Now move it from DEV to PROD for other users to consume
--------------------------------------------------------------------------------*/

-- clone the weather table into production
create or replace table citibike.public.weather clone citibike_dev.public.weather;

-- clean up the DEV environment and set context to PROD
drop database citibike_dev;
drop warehouse dev_wh;
use schema citibike.public;
use warehouse load_wh;


/*--------------------------------------------------------------------------------
  Create a view with trip (structured) and weather (semistructured) data
--------------------------------------------------------------------------------*/

-- note the complex SQL - we support CTEs and UDFs
create or replace secure view trips_weather_vw as (
  with
    t as (
      select date_trunc(hour, starttime) starttime, date_trunc(hour, stoptime) stoptime,
        tripduration, start_station_name, start_station_latitude, start_station_longitude,
        end_station_name, end_station_latitude, end_station_longitude, bikeid, usertype,
        birth_year, gender, program_name
      from trips_vw),
    w as (
      select date_trunc(hour, t)                observation_time
        ,avg(degKtoC(v:main.temp::float))       temp_avg_c
        ,min(degKtoC(v:main.temp_min::float))   temp_min_c
        ,max(degKtoC(v:main.temp_max::float))   temp_max_c
        ,avg(degKtoF(v:main.temp::float))       temp_avg_f
        ,min(degKtoF(v:main.temp_min::float))   temp_min_f
        ,max(degKtoF(v:main.temp_max::float))   temp_max_f
        ,avg(v:wind.deg::float)                 wind_dir
        ,avg(v:wind.speed::float)               wind_speed
      from weather
      where v:city.id::int = 5128638
      group by 1)
  select *
  from t left outer join w on t.starttime = w.observation_time);

-- check the results
select count(*) from trips_weather_vw;

select * from trips_weather_vw where year(observation_time) = 2019 limit 100;


/*--------------------------------------------------------------------------------
  Now Jane can analyse the integrated trip and weather data in Tableau.
--------------------------------------------------------------------------------*/
