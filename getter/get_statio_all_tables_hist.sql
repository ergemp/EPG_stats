drop function If EXISTS fv_stats.get_statio_all_tables_hist;

CREATE OR replace FUNCTION fv_stats.get_statio_all_tables_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    relid oid,
    schemaname character varying,
    relname character varying,
    heap_blks_read bigint,
    heap_blks_hit bigint,
    idx_blks_read bigint,
    idx_blks_hit bigint,
    toast_blks_read bigint,
    toast_blks_hit bigint,
    tidx_blks_read bigint,
    tidx_blks_hit bigint
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(sath.ts) AS ts, max(sath.ts) AS ts, sath.relid, sath.schemaname, sath.relname, 
      max(sath.heap_blks_read)-min(sath.heap_blks_read) as heap_blks_read,
      max(sath.heap_blks_hit)-min(sath.heap_blks_hit) as heap_blks_hit,
      max(sath.idx_blks_read)-min(sath.idx_blks_read) as idx_blks_read,
      max(sath.idx_blks_hit)-min(sath.idx_blks_hit) as idx_blks_hit,
      max(sath.toast_blks_read)-min(sath.toast_blks_read) as toast_blks_read,      
      max(sath.toast_blks_read)-min(sath.toast_blks_read) as toast_blks_read,
      max(sath.tidx_blks_read)-min(sath.tidx_blks_read) as tidx_blks_read,
      max(sath.tidx_blks_hit)-min(sath.tidx_blks_hit) as tidx_blks_hit
    from 
      fv_stats.statio_all_tables_hist  sath
      --where sath.ts in (select fv_stats.find_between(g_ts))
      WHERE sath.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
      --and sath.relname in ('pg_namespace')
    group by sath.relid, sath.schemaname, sath.relname;
    
END
$$
LANGUAGE plpgsql