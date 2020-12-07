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
BEGIN
    RETURN QUERY 
    select * from 
    (
    select 
      min(ssh.ts), max(ssh.ts), 
      ssh.userid, ssh.dbid, ssh.queryid, ssh.query, 
      max(ssh.calls) - min(ssh.calls) as calls, 
      max(ssh.total_time) - min(ssh.total_time),
      max(ssh.min_time) - min(ssh.min_time),
      max(ssh.max_time) - min(ssh.max_time),
      max(ssh.mean_time) - min(ssh.mean_time),
      max(ssh.stddev_time) - min(ssh.stddev_time),
      max(ssh.rows) - min(ssh.rows),
      max(ssh.shared_blks_hit) - min(ssh.shared_blks_hit),
      max(ssh.shared_blks_read) - min(ssh.shared_blks_read),
      max(ssh.shared_blks_dirtied) - min(ssh.shared_blks_dirtied),
      max(ssh.shared_blks_written) - min(ssh.shared_blks_written),
      max(ssh.local_blks_hit) - min(ssh.local_blks_hit),
      max(ssh.local_blks_read) - min(ssh.local_blks_read),
      max(ssh.local_blks_dirtied) - min(ssh.local_blks_dirtied),
      max(ssh.local_blks_written) - min(ssh.local_blks_written),      
      max(ssh.temp_blks_read) - min(ssh.temp_blks_read),
      max(ssh.temp_blks_written) - min(ssh.temp_blks_written),      
      max(ssh.blk_read_time) - min(ssh.blk_read_time),
      max(ssh.blk_write_time) - min(ssh.blk_write_time)      
    from 
      fv_stats.stat_statements_hist  ssh
    --where ssh.ts in (select fb.ts FROM fv_stats.find_between(g_ts) fb)        
    WHERE ssh.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
    GROUP BY ssh.userid, ssh.dbid, ssh.queryid, ssh.query
    ) as tt
    where tt.calls > 0
    ;    
END
$$
LANGUAGE plpgsql