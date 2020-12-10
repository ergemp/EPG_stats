drop function IF EXISTS fv_stats.get_stat_locks_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_locks_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    ts bigint,
    locktype text,
    database oid,
    relation oid,
    page integer,
    tuple smallint,
    virtualxid text,
    transactionid xid,
    classid oid,
    objid oid,
    objsubid smallint,
    virtualtransaction text,
    pid integer,
    mode text,
    granted boolean,
    fastpath boolean
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      slh.ts, slh.locktype, slh.database, slh.relation,
      slh.page, slh.tuple, slh.virtualxid, 
      slh.transactionid, slh.classid, slh.objid, 
      slh.objsubid, slh.virtualtransaction, slh.pid, slh.mode, 
      slh. granted, slh.fastpath
    from 
      fv_stats.stat_locks_hist  slh
    --WHERE slh.ts IN (select fb.ts from fv_stats.find_interval(g_ts, g_interval) fb)
    WHERE slh.ts BETWEEN
      (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
      and 
      (select max(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb)   
    ;    
END
$$
LANGUAGE plpgsql