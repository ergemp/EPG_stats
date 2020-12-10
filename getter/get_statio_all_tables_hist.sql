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
      max(sath.heap_blks_read) - case when coalesce(min(sath.heap_blks_read),0)=max(sath.heap_blks_read) then 0 else coalesce(min(sath.heap_blks_read),0) end as heap_blks_read,
      max(sath.heap_blks_hit) - case when coalesce(min(sath.heap_blks_hit),0)=max(sath.heap_blks_hit) then 0 else coalesce(min(sath.heap_blks_hit),0) end as heap_blks_hit,
      max(sath.idx_blks_read) - case when coalesce(min(sath.idx_blks_read),0)=max(sath.idx_blks_read) then 0 else coalesce(min(sath.idx_blks_read),0) end as idx_blks_read,
      max(sath.idx_blks_hit) - case when coalesce(min(sath.idx_blks_hit),0)=max(sath.idx_blks_hit) then 0 else coalesce(min(sath.idx_blks_hit),0) end as idx_blks_hit,
      max(sath.toast_blks_read) - case when coalesce(min(sath.toast_blks_read),0)=max(sath.toast_blks_read) then 0 else coalesce(min(sath.toast_blks_read),0) end as toast_blks_read,      
      max(sath.toast_blks_hit) - case when coalesce(min(sath.toast_blks_hit),0)=max(sath.toast_blks_hit) then 0 else coalesce(min(sath.toast_blks_hit),0) end as toast_blks_hit,
      max(sath.tidx_blks_read) - case when coalesce(min(sath.tidx_blks_read),0)=max(sath.tidx_blks_read) then 0 else coalesce(min(sath.tidx_blks_read),0) end as tidx_blks_read,
      max(sath.tidx_blks_hit) - case when coalesce(min(sath.tidx_blks_hit),0)=max(sath.tidx_blks_hit) then 0 else coalesce(min(sath.tidx_blks_hit),0) end as tidx_blks_hit
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