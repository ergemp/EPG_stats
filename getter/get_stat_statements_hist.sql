drop FUNCTION IF EXISTS  fv_stats.get_stat_statements_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_statements_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    userid oid,
    dbid oid,
    queryid bigint,
    query text,
    calls bigint,
    total_time double precision,
    min_time double precision,
    max_time double precision,
    mean_time double precision,
    stddev_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    local_blks_hit bigint,
    local_blks_read bigint,
    local_blks_dirtied bigint,
    local_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time double precision,
    blk_write_time double precision
)
AS 
$$
DECLARE 
  mints bigint;
  maxts bigint;
BEGIN

    select min(fb.ts) into mints from fv_stats.find_interval(g_ts, g_interval) fb;
    select min(fb.ts) into maxts from fv_stats.find_interval(g_ts, g_interval) fb;

    RETURN QUERY 
    select * from 
    (
    select 
      min(ssh.ts) as begin_ts, max(ssh.ts) as end_ts, 
      ssh.userid as userid, ssh.dbid as dbid, ssh.queryid as queryid, ssh.query as query, 
      max(ssh.calls) - case when coalesce(min(ssh.calls),0)=max(ssh.calls) then 0 else coalesce(min(ssh.calls),0) end as calls, 
      max(ssh.total_time) - case when coalesce(min(ssh.total_time),0)=max(ssh.total_time) then 0 else coalesce(min(ssh.total_time),0) end as total_time,
      max(ssh.min_time) - case when coalesce(min(ssh.min_time),0)=max(ssh.min_time) then 0 else coalesce(min(ssh.min_time),0) end as min_time  ,
      max(ssh.max_time) - case when coalesce(min(ssh.max_time),0)=max(ssh.max_time) then 0 else coalesce(min(ssh.max_time),0) end as max_time,
      max(ssh.mean_time) - case when coalesce(min(ssh.mean_time),0)=max(ssh.mean_time) then 0 else coalesce(min(ssh.mean_time),0) end as mean_time,
      max(ssh.stddev_time) - case when coalesce(min(ssh.stddev_time),0)=max(ssh.stddev_time) then 0 else coalesce(min(ssh.stddev_time),0) end as stddev_time,
      max(ssh.rows) - case when coalesce(min(ssh.rows),0)=max(ssh.rows) then 0 else coalesce(min(ssh.rows),0) end as rows,
      max(ssh.shared_blks_hit) - case when coalesce(min(ssh.shared_blks_hit),0)=max(ssh.shared_blks_hit) then 0 else coalesce(min(ssh.shared_blks_hit),0) end as shared_blks_hit,
      max(ssh.shared_blks_read) - case when coalesce(min(ssh.shared_blks_read),0)=max(ssh.shared_blks_read) then 0 else coalesce(min(ssh.shared_blks_read),0) end as shared_blks_read,
      max(ssh.shared_blks_dirtied) - case when coalesce(min(ssh.shared_blks_dirtied),0)=max(ssh.shared_blks_dirtied) then 0 else coalesce(min(ssh.shared_blks_dirtied),0) end as shared_blks_dirtied,
      max(ssh.shared_blks_written) - case when coalesce(min(ssh.shared_blks_written),0)=max(ssh.shared_blks_written) then 0 else coalesce(min(ssh.shared_blks_written),0) end as shared_blks_written,
      max(ssh.local_blks_hit) - case when coalesce(min(ssh.local_blks_hit),0)=max(ssh.local_blks_hit) then 0 else coalesce(min(ssh.local_blks_hit),0) end as local_blks_hit,
      max(ssh.local_blks_read) - case when coalesce(min(ssh.local_blks_read),0)=max(ssh.local_blks_read) then 0 else coalesce(min(ssh.local_blks_read),0) end as local_blks_read,
      max(ssh.local_blks_dirtied) - case when coalesce(min(ssh.local_blks_dirtied),0)=max(ssh.local_blks_dirtied) then 0 else coalesce(min(ssh.local_blks_dirtied),0) end as local_blks_dirtied,
      max(ssh.local_blks_written) - case when coalesce(min(ssh.local_blks_written),0)=max(ssh.local_blks_written) then 0 else coalesce(min(ssh.local_blks_written),0) end as local_blks_written,      
      max(ssh.temp_blks_read) - case when coalesce(min(ssh.temp_blks_read),0)=max(ssh.temp_blks_read) then 0 else coalesce(min(ssh.temp_blks_read),0) end as temp_blks_read,
      max(ssh.temp_blks_written) - case when coalesce(min(ssh.temp_blks_written),0)=max(ssh.temp_blks_written) then 0 else coalesce(min(ssh.temp_blks_written),0) end as temp_blks_written,      
      max(ssh.blk_read_time) - case when coalesce(min(ssh.blk_read_time),0)=max(ssh.blk_read_time) then 0 else coalesce(min(ssh.blk_read_time),0) end as blk_read_time,
      max(ssh.blk_write_time) - case when coalesce(min(ssh.blk_write_time),0)=max(ssh.blk_write_time) then 0 else coalesce(min(ssh.blk_write_time),0) end as blk_write_time 
    from 
      fv_stats.stat_statements_hist  ssh
    --where ssh.ts in (select fb.ts FROM fv_stats.find_between(g_ts) fb)        
    where ssh.ts between 
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)
    GROUP BY ssh.userid, ssh.dbid, ssh.queryid, ssh.query
    ) as tt
    where tt.calls > 0
    ;    
END
$$
LANGUAGE plpgsql