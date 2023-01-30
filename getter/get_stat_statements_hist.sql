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
      max(ssh.calls) - coalesce(min(ssh.calls),0) as calls, 
      max(ssh.total_time) - coalesce(min(ssh.total_time),0) as total_time,
      max(ssh.min_time) as min_time  ,
      max(ssh.max_time) as max_time,
      max(ssh.mean_time) as mean_time,
      max(ssh.stddev_time) as stddev_time,
      max(ssh.rows) -  coalesce(min(ssh.rows),0)  as rows,
      max(ssh.shared_blks_hit) -  coalesce(min(ssh.shared_blks_hit),0)  as shared_blks_hit,
      max(ssh.shared_blks_read) -  coalesce(min(ssh.shared_blks_read),0)  as shared_blks_read,
      max(ssh.shared_blks_dirtied) - coalesce(min(ssh.shared_blks_dirtied),0)  as shared_blks_dirtied,
      max(ssh.shared_blks_written) -  coalesce(min(ssh.shared_blks_written),0)  as shared_blks_written,
      max(ssh.local_blks_hit) - coalesce(min(ssh.local_blks_hit),0) as local_blks_hit,
      max(ssh.local_blks_read) - coalesce(min(ssh.local_blks_read),0) as local_blks_read,
      max(ssh.local_blks_dirtied) - coalesce(min(ssh.local_blks_dirtied),0) as local_blks_dirtied,
      max(ssh.local_blks_written) - coalesce(min(ssh.local_blks_written),0) as local_blks_written,      
      max(ssh.temp_blks_read) - coalesce(min(ssh.temp_blks_read),0) as temp_blks_read,
      max(ssh.temp_blks_written) - coalesce(min(ssh.temp_blks_written),0) as temp_blks_written,      
      max(ssh.blk_read_time) - coalesce(min(ssh.blk_read_time),0) as blk_read_time,
      max(ssh.blk_write_time) - coalesce(min(ssh.blk_write_time),0) as blk_write_time 
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