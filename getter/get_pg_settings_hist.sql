drop function IF EXISTS fv_stats.get_pg_settings_hist;

CREATE OR replace FUNCTION fv_stats.get_pg_settings_hist(g_ts bigint, g_interval interval) RETURNS TABLE 
(
    ts bigint,
    name text,
    setting text,
    category text
)
AS 
$$
BEGIN
    RETURN QUERY 
    select 
      psh.ts, psh.name, psh.setting, psh.category
    from 
      fv_stats.pg_settings_hist  psh
    WHERE psh.ts IN (select min(fb.ts) from fv_stats.find_interval(g_ts, g_interval) fb) 
    ;    
END
$$
LANGUAGE plpgsql