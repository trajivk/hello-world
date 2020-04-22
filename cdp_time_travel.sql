/*--------------------------------------------------------------------------------
  CDP & TIME TRAVEL V2

  #4 in the core demo flow.
  Run this in your demo account.

  Shows CDP through UNDROP and time travel to recover deleted records.

  Author:   Alan Eldridge
  Updated:  29 June 2019

  #cdp #timetravel #undrop #cdc #clone #recovery
--------------------------------------------------------------------------------*/

use role dba_citibike;
create warehouse if not exists load_wh with warehouse_size = 'medium' auto_suspend = 300 initially_suspended = true;
alter warehouse if  exists load_wh set warehouse_size = 'medium';
use warehouse load_wh;
use schema citibike.public;

/*--------------------------------------------------------------------------------
  Finally, we look at how continuous data protection can help against disaster
--------------------------------------------------------------------------------*/

-- we've all had one of those oops moments...
select count(*) from trips;

drop table trips;

-- ta da! CDP to the rescue!
undrop table trips;

select count(*) from trips;


/*--------------------------------------------------------------------------------
  Time travel allows us to avoid more complex oops moments...

  Earlier, we deleted all the records where NAME IS NULL, but we want them back

  delete from trips where program_id in (select program_id from programs where program_name is null);

--------------------------------------------------------------------------------*/

-- this is the query the consumer account was running
select program_name, count(*) as "Num Trips"
  from trips_vw
  group by 1
  order by 2 desc;

-- get the query ID of the delete statement, hold the result in a SQL variable
set query_id =
  (select query_id
   from table(information_schema.query_history_by_user(result_limit => 100))
   where query_text like 'delete%' order by start_time desc limit 1);

select $query_id;

-- compare the pair - before and after
select
    (select count(*) from trips) current_table_state,
    (select count(*) from trips before (statement => $query_id)) earlier_table_state;


/*--------------------------------------------------------------------------------
  Two ways to recover the records...
--------------------------------------------------------------------------------*/
-- if there have been no subsequent changes to the table, we can
-- use a clone statement with time travel to restore the table
-- this is instantaneous, even with very large tables

create table trips_recover clone trips before (statement => $query_id);

alter table trips swap with trips_recover;
drop table trips_recover;


-- or...
-- if we have inserted new records, we can't just replace the table so we can
-- use a CDC style query to identify the NULL records from the old table state
-- and insert them back into the table

alter warehouse load_wh set warehouse_size='xlarge';

insert into trips (
  select * from trips before (statement => $query_id)   -- before the delete
  minus
  select * from trips);                                 -- current table state

alter warehouse load_wh set warehouse_size='medium';


/*--------------------------------------------------------------------------------
  ta da!
--------------------------------------------------------------------------------*/

select count(*) from trips;

select program_name, count(*) as "Num Trips"
  from trips_vw
  group by 1
  order by 2 desc;
