drop FUNCTION IS EXISTS  fv_stats.get_statio_all_indexes_hist;

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
      min(saih.ts) AS ts, max(saih.ts) AS ts, saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname,
      max(saih.idx_blks_read)-min(saih.idx_blks_read) as idx_blks_read,
      max(saih.idx_blks_hit)-min(saih.idx_blks_hit) as idx_blks_hit
    from 
      fv_stats.statio_all_indexes_hist  saih
      --where saih.ts in (select fv_stats.find_between(g_ts))
      WHERE saih.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
      --and sath.relname in ('pg_namespace')
    group by saih.relid, saih.indexrelid, saih.schemaname, saih.relname, saih.indexrelname ;
    
END
$$
LANGUAGE plpgsql