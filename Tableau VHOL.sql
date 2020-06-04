


---------------------------------------------------------------------------------------------------
-- JSON Tableau Logs VHOL
-- David A Spezia
-- 01-Oct-2019
---------------------------------------------------------------------------------------------------
--Query History
select * from "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" limit 10;

--Create Database, Schema, Warehouse Stage and File Format
create or replace database TABLEAU;

create or replace schema TABLEAU;

create or replace warehouse TABLEAU_WH WITH 
    WAREHOUSE_SIZE = 'MEDIUM' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 1 
    SCALING_POLICY = 'STANDARD';

--Internal Stage
create or replace STAGE TABLEAU_JSON;
show stages;
--External Stage
create or replace STAGE TABLEAU_JSON
    URL = 's3://snowflake-workshop-lab/VHOL_Analytics_Tableau';
show stages;

create or replace FILE FORMAT JSON
    TYPE = 'JSON' 
    COMPRESSION = 'AUTO' 
    ENABLE_OCTAL = FALSE 
    ALLOW_DUPLICATE = FALSE 
    STRIP_OUTER_ARRAY = TRUE 
    STRIP_NULL_VALUES = FALSE 
    IGNORE_UTF8_ERRORS = FALSE;

show File Formats;

ls @TABLEAU_JSON;  --Lists Files on the S3 Bucket

/*SnowSQL CLI for PUT
snowsql -a demo118
use schema Tableau.Tableau;
put file:///Users/dspezia/bin/Data/n*.json @TABLEAU_JSON parallel = 5 auto_compress = true;
*/

--Playin with Data on the STAGE
--Confirm and Kick Tires on Log
show stages;

ls @TABLEAU_JSON;

select $1 from @TABLEAU_JSON/nativeapi_vizqlserver_1-0_2019_09_28_00_00_00.json (file_format => json) limit 1;


--Timestamp Cast
select current_timestamp::timestamp_ltz(6);


--Fancy Tablular Query on JSON from Stage
select
    json_doc.$1::variant as json,
    $1:ts::timestamp as ts,
    to_date($1:ts::timestamp) as date,
    to_time($1:ts::timestamp) as time,
    $1:pid::integer as pid,
    $1:tid::string as tid,
    $1:sev::string as sev,
    $1:req::string as req,
    $1:sess::string as sess,
    $1:site::string as site,
    $1:v:jobs[0]:"owner-dashboard"::string as view,
    $1:v:jobs[0]:"owner-worksheet"::string as sheet,
    $1:v:jobs[0]:"owner-component"::string as qptype
from
    @TABLEAU_JSON/nativeapi_vizqlserver_1-0_2019_09_28_00_00_00.json (file_format => json) json_doc
where
    $1:k::string = 'qp-batch-summary';

--Super Fancy Tabular Query on JSON from Stage
select
    json_doc.$1::variant as json,
    json_doc.$1:ts::timestamp as ts,
    to_date(json_doc.$1:ts::timestamp) as date,
    to_time(json_doc.$1:ts::timestamp) as time,
    json_doc.$1:pid::integer as pid,
    json_doc.$1:tid::string as tid,
    json_doc.$1:sev::string as sev,
    json_doc.$1:req::string as req,
    json_doc.$1:sess::string as sess,
    json_doc.$1:site::string as site,
    jobs.value:"owner-dashboard"::string as view,
    jobs.value:"owner-worksheet"::string as sheet,
    jobs.value:"owner-component"::string as qptype,
    queries.value:"query-compiled"::string as query_text
from @TABLEAU_JSON/nativeapi_vizqlserver_1-0_2019_09_28_00_00_00.json (file_format => json) json_doc, 
    lateral flatten(input => parse_json(json_doc.$1:v:jobs)) jobs,
    lateral flatten(input => parse_json(jobs.value:queries)) queries
where
    json_doc.$1:k::string = 'qp-batch-summary';

--Create External Table from Stage
create or replace external table exttbl_Test( 
    timestamp timestamp_ltz(9) as (current_timestamp),
    date date as to_date($1:ts::timestamp),
    time time as to_time($1:ts::timestamp),
    pid integer as ($1:pid::integer),
    tid string as ($1:tid::string),
    sev string as ($1:sev::string),
    req string as ($1:req::string),
    sess string as ($1:sess::string),
    site string as ($1:site::string))
Auto_Refresh = False -- Would be True for SQS after PUT
Location = @TABLEAU_JSON/
File_Format = (type = json, File_Extension = 'json');

select * from EXTTBL_TEST limit 100;


--Write VizQL Logs to Staging Table
create or replace sequence counter start = 1 increment = 1;

create or replace table stg_Logs (
    id_json number,
    log_json variant,
    dts_json timestamp_ltz(9)
);

copy into stg_Logs FROM (select counter.nextval, $1, current_timestamp from @TABLEAU_JSON/ (file_format => json)) on_error = skip_file;

select * from stg_Logs limit 10;

select * from stg_Logs where log_json:k::string = 'qp-batch-summary' limit 10;

--VizQL Lumberjack Data Format 2018.2(+)
create or replace sequence i start = 1 increment = 1;

create or replace table tbl_Snowjack as (
select
    id_json as id,
    log_json:ts::timestamp as ts,
    to_date(log_json:ts::timestamp) as date,
    to_time(log_json:ts::timestamp) as time,
    log_json:pid::string as pid,
    log_json:tid::string as tid,
    log_json:sev::string as sev,
    log_json:req::string as req,
    log_json:sess::string as sess,
    log_json:site::string as site,
    log_json:user::string as user, --//Local Handle Needed
    log_json:k::string as Key,
    log_json:v:elapsed::double as elapsed,
    log_json:v:"elapsed-sum"::double as elasped_sum,
    log_json:v:"job-count"::integer as job_count,
    jobs.value:"query-id"::integer as query_id,
    iff(1 + regexp_count(jobs.value,'},{') = row_number() over(partition by id, jobs.value:"query-id" order by 23) ,
        jobs.value:elapsed::double
    ,
        0
    ) as elaspsed_query,
    jobs.value:"owner-dashboard"::string as view,
    jobs.value:"owner-worksheet"::string as sheet,
    jobs.value:"owner-component"::string as qptype,
    queries.value:"cache-hit"::string as cache_hit,
    row_number() over(partition by id, jobs.value:"query-id" order by 23) as query_subid,
    i.nextval as query_uid,
    queries.value:"protocol-id"::integer as protocol_id,
    queries.value:"query-compiled"::string as query_text
from
    stg_Logs, lateral flatten(log_json:v:jobs) jobs, lateral flatten(jobs.value:queries) queries
where
    log_json:k::string = 'qp-batch-summary'
order by id,23);


--Select
select * from tbl_Snowjack limit 10;

select * from tbl_Snowjack where PID = '13276';


--Materialized View on Sessions
SELECT
    User,
    Sess as Session,
    MIN(TS) as Start_Time,
    MAX(TS) as End_Time,
    TIMESTAMPDIFF(seconds,MIN(TS),MAX(TS)) As Duration,
    COUNT(query_subid) as Queries
FROM
    tbl_Snowjack
GROUP BY
    User,
    Sess
ORDER BY
    Queries Desc;
    
create or replace materialized view mv_Snowjack_Sessions
    (User, Session, Start_Time, End_Time, Queries)
as
    SELECT User, Sess as Session, MIN(TS) as Start_Time, MAX(TS) as End_Time, COUNT(query_subid) as Queries
    FROM tbl_Snowjack GROUP BY User, Sess;

select *, TIMESTAMPDIFF(seconds,Start_Time,End_Time) As Duration from mv_Snowjack_Sessions;