drop function if exists fv_stats.get_stat_database_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_database_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    datid oid,
    datname character varying,
    numbackends integer,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    tup_returned bigint,
    tup_fetched bigint,
    tup_inserted bigint,
    tup_updated bigint,
    tup_deleted bigint,
    conflicts bigint,
    temp_files bigint,
    temp_bytes bigint,
    deadlocks bigint,
    checksum_failures bigint,
    checksum_last_failure timestamp with time zone,
    blk_read_time double precision,
    blk_write_time double precision,
    stats_reset timestamp with time zone
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(sdh.ts) as begin_ts, 
      max(sdh.ts) as end_ts, 
      sdh.datid, sdh.datname,
      max(sdh.numbackends) - coalesce(min(sdh.numbackends),0) AS numbackends,
      max(sdh.xact_commit) - coalesce(min(sdh.xact_commit),0) AS xact_commit,
      max(sdh.xact_rollback) - coalesce(min(sdh.xact_rollback),0) AS xact_rollback,
      max(sdh.blks_read) - coalesce(min(sdh.blks_read),0) AS blks_read,
      max(sdh.blks_hit) - coalesce(min(sdh.blks_hit),0) AS blks_hit,
      max(sdh.tup_returned) - coalesce(min(sdh.tup_returned),0) AS tup_returned,
      max(sdh.tup_fetched) - coalesce(min(sdh.tup_fetched),0) AS tup_fetched,
      max(sdh.tup_inserted) - coalesce(min(sdh.tup_inserted),0) AS tup_inserted,
      max(sdh.tup_updated) - coalesce(min(sdh.tup_updated),0) AS tup_updated,
      max(sdh.tup_deleted) - coalesce(min(sdh.tup_deleted),0) AS tup_deleted,      
      max(sdh.conflicts) - coalesce(min(sdh.conflicts),0) AS conflicts,      
      max(sdh.temp_files) - coalesce(min(sdh.temp_files),0) AS temp_files,      
      max(sdh.temp_bytes) - coalesce(min(sdh.temp_bytes),0) AS temp_bytes,      
      max(sdh.deadlocks) - coalesce(min(sdh.deadlocks),0) AS deadlocks,      
      max(sdh.checksum_failures) - coalesce(min(sdh.checksum_failures),0) AS checksum_failures,      
      max(sdh.checksum_last_failure),      
      max(sdh.blk_read_time) - coalesce(min(sdh.blk_read_time),0) AS blk_read_time,      
      max(sdh.blk_write_time) - coalesce(min(sdh.blk_write_time),0) AS blk_write_time,      
      max(sdh.stats_reset)      
    from 
      fv_stats.stat_database_hist  sdh
    --WHERE sdh.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
    WHERE sdh.ts BETWEEN
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
    GROUP BY sdh.datid, sdh.datname
    ;    
END
$$
LANGUAGE plpgsql