drop function if exists fv_stats.find_interval(bigint, interval);

CREATE OR replace FUNCTION fv_stats.find_interval(g_ts bigint, g_interval interval) RETURNS TABLE (ts bigint) AS 
$$
DECLARE 
  act_ts bigint;
  begin_ts bigint;
  end_ts bigint;
BEGIN
  act_ts := EXTRACT(epoch FROM (to_timestamp(g_ts) - g_interval));
  
  --SELECT min(ts), min(to_timestamp(ts)) FROM fv_stats.find_between(cast(extract (epoch from (now()-interval '1 hour')) as bigint));
  --SELECT max(ts), max(to_timestamp(ts)) FROM fv_stats.find_between(cast(extract (epoch from (now())) as bigint));
  
  SELECT min(fb.ts) INTO begin_ts FROM fv_stats.find_between(cast(extract (epoch from (to_timestamp(g_ts)-g_interval)) as bigint)) fb;
  SELECT max(fb.ts) INTO end_ts FROM fv_stats.find_between(cast(extract (epoch from (to_timestamp(g_ts))) as bigint)) fb;

  if (begin_ts = end_ts) then 
    select min(sah.ts) into begin_ts from fv_stats.stat_activity_hist sah;
    select max(sah.ts) into begin_ts from fv_stats.stat_activity_hist sah;
  end if;

  ts := begin_ts;
  RETURN NEXT;
  ts := end_ts;
  RETURN NEXT;  
END
$$
LANGUAGE plpgsql

--select to_timestamp(fv_stats.find_between(1606112710));