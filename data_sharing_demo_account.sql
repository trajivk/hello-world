/*--------------------------------------------------------------------------------

  DATA SHARING DEMO ACCOUNT V2

  #3a in the core demo flow.
  Run this in your demo account.

  This is the publisher side of the data sharing demo.
  We consume the incoming share and query the data. We ask for the null data
  to be cleaned up. Then we query the restricted share.

  Author:   Alan Eldridge
  Updated:  10 June 2019

  #datasharing #publisher #managed_account #secure
--------------------------------------------------------------------------------*/

use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'medium' auto_suspend = 300 initially_suspended = true;
alter warehouse if  exists load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.public;

/*--------------------------------------------------------------------------------
  We have created value-added data by combining the trip and the weather data.
  We want to share this combined data with other organisations that use Citibike
  e.g. NYCHA, JCHA - housing authorities that offer discount programs to their members
--------------------------------------------------------------------------------*/

create or replace share citibike comment='Share trip data with housing authorities.';

--what are we sharing?
grant usage on database citibike to share citibike;
grant usage on schema citibike.public to share citibike;
grant select on view citibike.public.trips_weather_vw to share citibike;

-- who are we sharing with?
-- we have created a security table with accounts and filter rules
select * from security;

-- grant access to NYCHA and JCHA accounts
set nycha = (select account from security where name = 'NYCHA');
set jcha = (select account from security where name = 'JCHA');

alter share citibike add accounts = $nycha, $jcha;

-- check the share
show shares like 'citibike';
desc share citibike;


/*--------------------------------------------------------------------------------
  Now it's time to consume the share
  ==>
  ==> switch over to the NYCHA account and connect to the share
  ==>
--------------------------------------------------------------------------------*/


-- remove the NULL records reported by the consumer
delete from trips where program_id in
  (select program_id from programs where program_name is null);


/*--------------------------------------------------------------------------------
  The nulls are gone
  ==>
  ==> switch back to the consumer account and query the data
  ==>
--------------------------------------------------------------------------------*/


-- create a restricted view that limits the amount of data available based on the
-- account of the viewer. It also selectively obfuscates two of the columns.
create or replace secure view secure_trips_weather_vw as
  select current_account() as acct,
         program_name,
         date_trunc(hour, starttime) starttime_hr,
         iff(current_account() in (select account from security where name = 'Publisher Account'),
             start_station_name, 'REDACTED (' || md5(start_station_name) || ')') start_station_name,
         iff(current_account() in (select account from security where name = 'Publisher Account'),
             end_station_name, 'REDACTED (' || md5(end_station_name) || ')') end_station_name,
         temp_avg_c, temp_avg_f, wind_speed
  from trips_weather_vw inner join security
  where program_name like filter
    and account = current_account();


-- test that the security works
select * from secure_trips_weather_vw limit 100;

select program_name, acct, count(*) as "Num Trips"
  from secure_trips_weather_vw
  group by 1,2
  order by 3 desc;

alter session set simulated_data_sharing_consumer = $nycha;
alter session set simulated_data_sharing_consumer = $jcha;

alter session unset simulated_data_sharing_consumer;


-- change the share to use the secured view instead
revoke select on view citibike.public.trips_weather_vw from share citibike;
grant select on view citibike.public.secure_trips_weather_vw to share citibike;

desc share citibike;


/*--------------------------------------------------------------------------------
  The data we are sharing is now locked down per consumer
  ==>
  ==> switch back to the NYCHA account and query the data
  ==>
--------------------------------------------------------------------------------*/
