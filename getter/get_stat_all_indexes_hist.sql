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
      min(saih.ts) AS ts, max(saih.ts) AS ts, saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname,
      max(saih.idx_scan)-min(saih.idx_scan) as idx_scan,
      max(saih.idx_tup_read)-min(saih.idx_tup_read) as idx_tup_read,
      max(saih.idx_tup_fetch)-min(saih.idx_tup_fetch) as idx_tup_fetch
    from 
      fv_stats.stat_all_indexes_hist  saih
    --where saih.ts in (select fv_stats.find_between(g_ts))
    WHERE saih.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
      --and sath.relname in ('pg_namespace')
    group by saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname ;
    
END
$$
LANGUAGE plpgsql