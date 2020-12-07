drop FUNCTION IF exists fv_stats.get_stat_activity_hist;

CREATE OR replace FUNCTION fv_stats.get_stat_activity_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    ts bigint,
    datid oid,
    datname character varying,
    pid integer,
    usesysid oid,
    usename character varying,
    application_name text,
    client_addr inet,
    client_hostname text,
    client_port integer,
    backend_start timestamp with time zone,
    xact_start timestamp with time zone,
    query_start timestamp with time zone,
    state_change timestamp with time zone,
    wait_event_type text,
    wait_event text,
    state text,
    backend_xid xid,
    backend_xmin xid,
    query text,
    backend_type text
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      sah.ts, sah.datid, sah.datname, sah.pid,
      sah.usesysid, sah.usename, sah.application_name, 
      sah.client_addr, sah.client_hostname, sah.client_port, 
      sah.backend_start, sah.xact_start, sah.query_start, sah.state_change, 
      sah.wait_event_type, sah.wait_event, sah.state, 
      sah.backend_xid, sah.backend_xmin, sah.query, 
      sah.backend_type 
    from 
      fv_stats.stat_activity_hist  sah
    --where sah.ts in (select min(fb.ts) FROM fv_stats.find_between(g_ts) fb)      
    WHERE sah.ts IN (select fb.ts from fv_stats.find_interval(g_ts, g_interval) fb)
    ;    
END
$$
LANGUAGE plpgsql