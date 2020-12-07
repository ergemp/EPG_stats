drop function IF EXISTS fv_stats.get_stat_archiver_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_archiver_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    archived_count bigint,
    last_archived_wal text,
    last_archived_time timestamp with time zone,
    failed_count bigint,
    last_failed_wal text,
    last_failed_time timestamp with time zone,
    stats_reset timestamp with time zone
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(sah.ts), max(sah.ts), 
      max(sah.archived_count) - min(sah.archived_count) AS archived_count,
      max(sah.last_archived_wal),
      max(sah.last_archived_time),
      max(sah.failed_count) - min(sah.failed_count) AS failed_count,
      max(sah.last_failed_wal),
      max(sah.last_failed_time),
      max(sah.stats_reset)      
    from 
      fv_stats.stat_archiver_hist  sah
    --where sah.ts in (select * FROM fv_stats.find_between(g_ts) fb)   
    WHERE sah.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
    ;    
END
$$
LANGUAGE plpgsql