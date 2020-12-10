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
      max(saih.idx_scan) - case when coalesce(min(saih.idx_scan),0)=max(saih.idx_scan) then 0 else coalesce(min(saih.idx_scan),0) end as idx_scan,
      max(saih.idx_tup_read) - case when coalesce(min(saih.idx_tup_read),0)=max(saih.idx_tup_read) then 0 else coalesce(min(saih.idx_tup_read),0) end as idx_tup_read,
      max(saih.idx_tup_fetch) - case when coalesce(min(saih.idx_tup_fetch),0)=max(saih.idx_tup_fetch) then 0 else coalesce(min(saih.idx_tup_fetch),0) end as idx_tup_fetch
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