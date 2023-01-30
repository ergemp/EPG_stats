drop FUNCTION If EXISTS  fv_stats.get_statio_all_indexes_hist;

CREATE OR replace FUNCTION fv_stats.get_statio_all_indexes_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    relid oid,
    indexrelid oid,
    schemaname character varying,
    relname character varying,
    indexrelname character varying,
    idx_blks_read bigint,
    idx_blks_hit bigint
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(saih.ts) AS begin_ts, 
      max(saih.ts) AS end_ts, 
      saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname,
      abs(max(saih.idx_blks_read) - coalesce(min(saih.idx_blks_read),0)) as idx_blks_read,
      abs(max(saih.idx_blks_hit) - coalesce(min(saih.idx_blks_hit),0)) as idx_blks_hit
    from 
      fv_stats.statio_all_indexes_hist  saih
      WHERE saih.ts BETWEEN
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)  
      --and sath.relname in ('pg_namespace')
    group by saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname ;
    
END
$$
LANGUAGE plpgsql
