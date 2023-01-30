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
      min(sath.ts) AS begin_ts, 
      max(sath.ts) AS end_ts, 
      sath.relid, sath.schemaname, sath.relname, 
      abs(max(sath.heap_blks_read) - coalesce(min(sath.heap_blks_read),0)) as heap_blks_read,
      abs(max(sath.heap_blks_hit) - coalesce(min(sath.heap_blks_hit),0)) as heap_blks_hit,
      abs(max(sath.idx_blks_read) - coalesce(min(sath.idx_blks_read),0)) as idx_blks_read,
      abs(max(sath.idx_blks_hit) - coalesce(min(sath.idx_blks_hit),0)) as idx_blks_hit,
      abs(max(sath.toast_blks_read) - coalesce(min(sath.toast_blks_read),0)) as toast_blks_read,      
      abs(max(sath.toast_blks_hit) - coalesce(min(sath.toast_blks_hit),0)) as toast_blks_hit,
      abs(max(sath.tidx_blks_read) - coalesce(min(sath.tidx_blks_read),0)) as tidx_blks_read,
      abs(max(sath.tidx_blks_hit) - coalesce(min(sath.tidx_blks_hit),0)) as tidx_blks_hit
    from 
      fv_stats.statio_all_tables_hist  sath
      WHERE sath.ts BETWEEN
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)        
      --and sath.relname in ('pg_namespace')
    group by sath.relid, sath.schemaname, sath.relname;
    
END
$$
LANGUAGE plpgsql