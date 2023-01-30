drop function IF EXISTS fv_stats.get_stat_all_indexes_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_all_indexes_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    relid oid,
    indexrelid oid,
    schemaname character varying,
    relname character varying,
    indexrelname character varying,
    idx_scan bigint,
    idx_tup_read bigint,
    idx_tup_fetch bigint
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(saih.ts) AS begin_ts, 
      max(saih.ts) AS end_ts, 
      saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname,
      abs(coalesce(max(saih.idx_scan),0) - coalesce(min(saih.idx_scan),0)) as idx_scan,
      abs(max(saih.idx_tup_read) - coalesce(min(saih.idx_tup_read),0)) as idx_tup_read,
      abs(max(saih.idx_tup_fetch) - coalesce(min(saih.idx_tup_fetch),0))  as idx_tup_fetch
    from 
      fv_stats.stat_all_indexes_hist  saih
    WHERE saih.ts BETWEEN
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)    
      --and sath.relname in ('pg_namespace')
    group by saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname ;
    
END
$$
LANGUAGE plpgsql