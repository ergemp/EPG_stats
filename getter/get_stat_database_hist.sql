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
      min(sdh.ts), max(sdh.ts), sdh.datid, sdh.datname,
      max(sdh.numbackends) - min(sdh.numbackends) AS numbackends,
      max(sdh.xact_commit) - min(sdh.xact_commit) AS xact_commit,
      max(sdh.xact_rollback) - min(sdh.xact_rollback) AS xact_rollback,
      max(sdh.blks_read) - min(sdh.blks_read) AS blks_read,
      max(sdh.blks_hit) - min(sdh.blks_hit) AS blks_hit,
      max(sdh.tup_returned) - min(sdh.tup_returned) AS tup_returned,
      max(sdh.tup_fetched) - min(sdh.tup_fetched) AS tup_fetched,
      max(sdh.tup_inserted) - min(sdh.tup_inserted) AS tup_inserted,
      max(sdh.tup_updated) - min(sdh.tup_updated) AS tup_updated,
      max(sdh.tup_deleted) - min(sdh.tup_deleted) AS tup_deleted,      
      max(sdh.conflicts) - min(sdh.conflicts) AS conflicts,      
      max(sdh.temp_files) - min(sdh.temp_files) AS temp_files,      
      max(sdh.temp_bytes) - min(sdh.temp_bytes) AS temp_bytes,      
      max(sdh.deadlocks) - min(sdh.deadlocks) AS deadlocks,      
      max(sdh.checksum_failures) - min(sdh.checksum_failures) AS checksum_failures,      
      max(sdh.checksum_last_failure),      
      max(sdh.blk_read_time) - min(sdh.blk_read_time) AS blk_read_time,      
      max(sdh.blk_write_time) - min(sdh.blk_write_time) AS blk_write_time,      
      max(sdh.stats_reset)      
    from 
      fv_stats.stat_database_hist  sdh
    --where sdh.ts in (select * FROM fv_stats.find_between(g_ts) fb)    
    WHERE sdh.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
    GROUP BY sdh.datid, sdh.datname
    ;    
END
$$
LANGUAGE plpgsql