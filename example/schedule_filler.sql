
--
-- scheduling pg_cron job
--
insert into cron.job 
(
jobid,
schedule,
command,
nodename,
nodeport,
database,
username,
active,
jobname
)
values
(
4,
'5/5 * * * *',
'call fv_stats.fill_meta();',
'localhost',
'5432',
'ergemp',
'postgres',
true,
'fv_stats_filler'
)
;


call fv_stats.fill_meta();

update cron.job set jobid=5 where jobid=4;

update cron.job set database='postgres' where jobid=5;
update cron.job set schedule='15,30,45,00 * * * *' where jobid=5;

select * from cron.job;
select * from cron.job_run_details order by runid desc;

--disable a job
update cron.job set active=false where jobid=5;


--
-- desc 
--
select column_name || ' ' || data_type || ',' from information_schema."columns"  where table_name = 'stat_statements_hist' order by ordinal_position asc;

--
-- work on epoch end datetime
--
select now();
select extract(epoch from now());

select to_timestamp(max(ts)), max(ts) from fv_stats.stat_activity_hist; 
select distinct to_timestamp(ts), ts from fv_stats.stat_activity_hist order by to_timestamp(ts) desc;


select to_timestamp(ts), ts from fv_stats.stat_activity_hist where ts >= 1606199201 order by ts asc limit 1;
select to_timestamp(ts), ts from fv_stats.stat_activity_hist where ts >= 1606199201 order by ts asc limit 1; 
select to_timestamp(ts), ts from fv_stats.stat_activity_hist where ts <= 1606199201 order by ts desc limit 1;

select extract(epoch from now());
select now(), (now()-interval '1 hour');

--
-- test find_between and find_interval
--
select * from fv_stats.stat_activity_hist where ts in (select fv_stats.find_between(1606199201));
select * from fv_stats.stat_all_tables_hist;

select * from fv_stats.find_between(cast(extract(epoch from now()) as bigint));

select ts, to_timestamp(ts) from fv_stats.find_between(1606112710) ;
select ts, to_timestamp(ts) from fv_stats.find_between(cast(extract(epoch from now()) as bigint)) ;

select min(ts) from fv_stats.find_between(1606112710) ;
select max(ts) from fv_stats.find_between(1606112710) ;

select min(ts) from fv_stats.find_between(cast(extract(epoch from now()) as bigint)) ;

SELECT min(ts), min(to_timestamp(ts)) FROM fv_stats.find_between(cast(extract (epoch from (now()-interval '1 hour')) as bigint));
SELECT max(ts), max(to_timestamp(ts)) FROM fv_stats.find_between(cast(extract (epoch from (now())) as bigint));

select ts, to_timestamp(ts) from fv_stats.find_interval(cast(extract (epoch from now()) as bigint),interval '1 day');
select ts, to_timestamp(ts) from fv_stats.find_interval(cast(extract (epoch from now()) as bigint),interval '1 hour');
select ts, to_timestamp(ts) from fv_stats.find_interval(1606199201,interval '20 mins')

select extract (epoch from to_timestamp('2020-11-24 10:00','YYYY-MM-DD HH:MI'))
select ts, to_timestamp(ts) from fv_stats.find_interval(now(),interval '30 min')

--
-- test getter functions
--  
select * from fv_stats.get_stat_bgwriter_hist(cast(extract(epoch from now()) as bigint)) ;
select * from fv_stats.get_stat_bgwriter_hist(1606199201) ;
  

select count(*) from 
(
  select * from fv_stats.stat_activity_hist sah where sah.ts <= cast(extract(epoch from now()) as bigint) limit 1
) as cnt;

select max(sah.ts), to_timestamp(max(sah.ts)) FROM fv_stats.stat_activity_hist sah WHERE sah.ts < (select max(ts) from fv_stats.stat_activity_hist );
  

select * from fv_stats.get_stat_statements_hist(1606199201) order by total_time desc ;  
select * from fv_stats.get_stat_statements_hist(1606199201) where queryid=-976633341077716839 order by total_time desc ;
select * from fv_stats.stat_statements_hist ssh  where ssh.ts = 1606199201 and ssh.queryid=-976633341077716839;
select * from fv_stats.stat_statements_hist ssh  where ssh.ts = 1606199201 and ssh.queryid=-976633341077716839;


select * from fv_stats.get_stat_all_tables_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_statio_all_tables_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_all_indexes_hist(1606199201,INTERVAL '20 mins');
select * from fv_stats.get_statio_all_indexes_hist(1606199201,INTERVAL '20 mins');
select * from fv_stats.get_stat_database_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_bgwriter_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_archiver_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_locks_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_statements_hist(1606199201,INTERVAL '20 mins') ;
select * from fv_stats.get_stat_activity_hist(1606199201,INTERVAL '20 mins') ;

SELECT 
  to_timestamp(begin_ts), to_timestamp(end_ts), * 
FROM fv_stats.get_stat_all_tables_hist(1606223700, interval '30 min') order by seq_scan desc

SELECT 
  to_timestamp(begin_ts), to_timestamp(end_ts), * 
FROM fv_stats.get_stat_all_indexes_hist(1606223700, interval '30 min') ;

select 
  to_timestamp(ts), * 
from fv_stats.get_stat_activity_hist(cast(extract (epoch from now()- interval '2 hour') as bigint),INTERVAL '30 mins') ;

select * from fv_stats.stat_bgwriter_hist sbh where ts = 1606197600

