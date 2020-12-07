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
      max(sath.seq_scan)-min(sath.seq_scan) as seq_scan,
      max(sath.seq_tup_read)-min(sath.seq_tup_read) as seq_tup_read,
      max(sath.idx_scan)-min(sath.idx_scan) as idx_scan,
      max(sath.idx_tup_fetch)-min(sath.idx_tup_fetch) as idx_tup_fetch,
      max(sath.n_tup_ins)-min(sath.n_tup_ins) as n_tup_ins,
      max(sath.n_tup_upd)-min(sath.n_tup_upd) as n_tup_upd,
      max(sath.n_tup_del)-min(sath.n_tup_del) as n_tup_del,
      max(sath.n_tup_hot_upd)-min(sath.n_tup_hot_upd) as n_tup_hot_upd,
      min(sath.n_live_tup) n_live_tup,
      min(sath.n_dead_tup) n_dead_tup,
      min(sath.n_mod_since_analyze) as n_mod_since_analyze,
      min(sath.last_vacuum) as last_vacuum,
      min(sath.last_autovacuum) as last_autovacuum,
      min(sath.last_analyze) as last_analyze,
      min(sath.last_autoanalyze) as last_autoanalyze,
      min(sath.vacuum_count) as vacuum_count,
      min(sath.autovacuum_count) as autovacuum_count,
      min(sath.analyze_count) as analyze_count,
      min(sath.autoanalyze_count) as autoanalyze_count
    from 
      fv_stats.stat_all_tables_hist  sath
    --where sath.ts in (select fv_stats.find_between(g_ts))
    WHERE sath.ts IN (select ts from fv_stats.find_interval(g_ts, g_interval))
      --and sath.relname in ('pg_namespace')
    group by sath.relid, sath.schemaname, sath.relname;
    
END
$$
LANGUAGE plpgsql