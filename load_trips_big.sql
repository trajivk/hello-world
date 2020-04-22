/*--------------------------------------------------------------------------------
  LOAD TRIPS BIG V2

  #1 in the core demo flow when you are running on the 1Bn record demo.
  Run this in your demo account.

  This script loads the TRIPS table from staged CSV files. It shows vertical
  scalability when changing the warehouse size, and then shows how Snowflake
  gives performance without needing to tune the data.

  Author:   Alan Eldridge
  Updated:  29 June 2019

  #trips #structured #loading #vertical #scalability #elasticity
  #performance #view #big #billion
--------------------------------------------------------------------------------*/

use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'xlarge' auto_suspend = 300 initially_suspended = true;
alter warehouse if  exists load_wh set warehouse_size = 'xlarge';
use warehouse load_wh;
use schema citibike.public;

/*--------------------------------------------------------------------------------
  We have the Citibike trip data pre-staged in a cloud bucket
--------------------------------------------------------------------------------*/

-- create the stage
create or replace stage trips
  url = 's3://citibike-demo-us-west/V2/big/trips'
  credentials = (aws_key_id = 'AKIAILDSC3QV7KIM3MNQ' aws_secret_key = 'gVB0McQsohfQbhQoBxM8vaOA1pHBZx1mKlgnu6va')
  file_format = csv_no_header;

  list @trips/;


  /*--------------------------------------------------------------------------------
    Load the data
  --------------------------------------------------------------------------------*/

  create or replace table trips (
  	tripduration integer,
  	starttime timestamp_ntz,
  	stoptime timestamp_ntz,
  	start_station_id integer,
  	end_station_id integer,
  	bikeid integer,
  	usertype string,
  	birth_year integer,
  	gender integer,
  	program_id integer);

  copy into trips from @trips/2019/;

  select count(*) from trips;


  /*--------------------------------------------------------------------------------
    But we have more data to load... another 5 years or so.
    We want this to load quickly, so we can elastically scale our warehouse
  --------------------------------------------------------------------------------*/

  alter warehouse load_wh set warehouse_size='3x-large';

  copy into trips from @trips;

  alter warehouse load_wh set warehouse_size='large';

  -- check the results
  select count(*) from trips;

  select * from trips limit 20;


  /*--------------------------------------------------------------------------------
    Get some answers!
  --------------------------------------------------------------------------------*/

  -- trip records link to our lookup tables
  select * from stations;
  select * from programs;

  -- in hourly groups, how many trips were taken, how long did they last, and
  -- how far did they ride?
  select
    date_trunc(hour, starttime) as "Hour",
    count(*) as "Num Trips",
    avg(tripduration)/60 as "Avg Duration (mins)",
    avg(haversine(ss.station_latitude, ss.station_longitude, es.station_latitude, es.station_longitude)) as "Avg Distance (Km)"
  from trips t inner join stations ss on t.start_station_id = ss.station_id
               inner join stations es on t.end_station_id = es.station_id
  where start_station_id < 200
  group by 1
  order by 4 desc;


  /*--------------------------------------------------------------------------------
    Let's make the demo a bit easier to read going forward...
    We create a view joining TRIPS to STATIONS and PROGRAMS
  --------------------------------------------------------------------------------*/

  create or replace secure view trips_vw as
  select tripduration, starttime, stoptime,
         ss.station_name start_station_name, ss.station_latitude start_station_latitude, ss.station_longitude start_station_longitude,
         es.station_name end_station_name, es.station_latitude end_station_latitude, es.station_longitude end_station_longitude,
         bikeid, usertype, birth_year, gender, program_name
  from trips t inner join stations ss on t.start_station_id = ss.station_id
               inner join stations es on t.end_station_id = es.station_id
               inner join programs p on t.program_id = p.program_id;

  select * from trips_vw where year(starttime)=2019 limit 20;


  /*--------------------------------------------------------------------------------
    Instead of writing SQL, Jane will query it via our BI toolset
  --------------------------------------------------------------------------------*/
