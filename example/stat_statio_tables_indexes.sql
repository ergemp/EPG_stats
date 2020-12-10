select 
  100 * seq_scan / case when (idx_scan + seq_scan) = 0 then 1 else (idx_scan + seq_scan) end as seq_scan_ratio, 
  schemaname, relname, seq_scan, idx_scan, seq_tup_read, idx_tup_fetch 
from fv_stats.get_stat_all_tables_hist( cast(extract (epoch from now()) as bigint), INTERVAL '5 days')
where schemaname not in ('information_schema','pg_catalog','pg_toast')
  AND idx_scan IS NOT NULL
  AND seq_scan IS NOT NULL
order by 1 desc
limit 20
;