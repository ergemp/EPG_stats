drop function IF EXISTS fv_stats.get_stat_all_tables_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_all_tables_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    begin_ts bigint,
    end_ts bigint,
    relid oid,
    schemaname character varying,
    relname character varying,
    seq_scan bigint,
    seq_tup_read bigint,
    idx_scan bigint,
    idx_tup_fetch bigint,
    n_tup_ins bigint,
    n_tup_upd bigint,
    n_tup_del bigint,
    n_tup_hot_upd bigint,
    n_live_tup bigint,
    n_dead_tup bigint,
    n_mod_since_analyze bigint,
    last_vacuum timestamp with time zone,
    last_autovacuum timestamp with time zone,
    last_analyze timestamp with time zone,
    last_autoanalyze timestamp with time zone,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      min(sath.ts) AS ts, max(sath.ts) AS ts, sath.relid, sath.schemaname, sath.relname, 
      max(sath.seq_scan) - case when coalesce(min(sath.seq_scan),0)=max(sath.seq_scan) then 0 else coalesce(min(sath.seq_scan),0) end as seq_scan,
      max(sath.seq_tup_read) - case when coalesce(min(sath.seq_tup_read),0)=max(sath.seq_tup_read) then 0 else coalesce(min(sath.seq_tup_read),0) end as seq_tup_read,
      max(sath.idx_scan) - case when coalesce(min(sath.idx_scan),0)=max(sath.idx_scan) then 0 else coalesce(min(sath.idx_scan),0) end as idx_scan,
      max(sath.idx_tup_fetch) - case when coalesce(min(sath.idx_tup_fetch),0)=max(sath.idx_tup_fetch) then 0 else coalesce(min(sath.idx_tup_fetch),0) end as idx_tup_fetch,
      max(sath.n_tup_ins) - case when coalesce(min(sath.n_tup_ins),0)=max(sath.n_tup_ins) then 0 else coalesce(min(sath.n_tup_ins),0) end as n_tup_ins,
      max(sath.n_tup_upd) - case when coalesce(min(sath.n_tup_upd),0)=max(sath.n_tup_upd) then 0 else coalesce(min(sath.n_tup_upd),0) end as n_tup_upd,
      max(sath.n_tup_del) - case when coalesce(min(sath.n_tup_del),0)=max(sath.n_tup_del) then 0 else coalesce(min(sath.n_tup_del),0) end as n_tup_del,
      max(sath.n_tup_hot_upd) - case when coalesce(min(sath.n_tup_hot_upd),0)=max(sath.n_tup_hot_upd) then 0 else coalesce(min(sath.n_tup_hot_upd),0) end as n_tup_hot_upd,
      max(sath.n_live_tup) n_live_tup,
      max(sath.n_dead_tup) n_dead_tup,
      max(sath.n_mod_since_analyze) as n_mod_since_analyze,
      max(sath.last_vacuum) as last_vacuum,
      max(sath.last_autovacuum) as last_autovacuum,
      max(sath.last_analyze) as last_analyze,
      max(sath.last_autoanalyze) as last_autoanalyze,
      max(sath.vacuum_count) as vacuum_count,
      max(sath.autovacuum_count) as autovacuum_count,
      max(sath.analyze_count) as analyze_count,
      max(sath.autoanalyze_count) as autoanalyze_count
    from 
      fv_stats.stat_all_tables_hist  sath
    WHERE sath.ts between 
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)     
      --and sath.relname in ('pg_namespace')
    group by sath.relid, sath.schemaname, sath.relname;
    
END
$$
LANGUAGE plpgsql