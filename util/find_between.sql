drop function if exists fv_stats.find_between(bigint);

CREATE OR replace FUNCTION fv_stats.find_between(g_ts bigint) RETURNS TABLE (ts bigint) AS 
$$
DECLARE 
  l_ts bigint;
  max_ts bigint;  
  prev_ts bigint;
  act_ts bigint;
BEGIN    
  act_ts := g_ts; 
  
  SELECT max(sah.ts) INTO max_ts FROM fv_stats.stat_activity_hist sah;
  SELECT max(sah.ts) INTO prev_ts FROM fv_stats.stat_activity_hist sah WHERE sah.ts < max_ts;
  
  IF max_ts <= g_ts THEN 
    act_ts := (prev_ts + max_ts)/2;
  END IF;
  
  select sah.ts INTO ts from fv_stats.stat_activity_hist sah where sah.ts >= act_ts order by ts asc limit 1;
  RETURN next;
  select sah.ts INTO ts from fv_stats.stat_activity_hist sah where sah.ts <= act_ts order by ts desc limit 1;
  RETURN NEXT;
END
$$
LANGUAGE plpgsql

--select to_timestamp(fv_stats.find_between(1606112710));