DROP FUNCTION IF EXISTS fv_stats.generate_report(bigint, interval, text);

CREATE OR replace PROCEDURE fv_stats.generate_report(g_ts bigint, g_interval interval, g_filename text) AS 
$$
DECLARE 
  act_ts bigint;
  begin_time timestamp;
  end_time timestamp;
  dbname text;
  dbcachehitratio numeric;
  tempfiles numeric;
  tempmbs numeric;
  totalcommits numeric;
  totalreadtimesec numeric;
  totalwritetimesec numeric;
  statslastreset timestamp with time zone;
  databasesize text;
  databaseblocksize text;

  currenttimestamp timestamp with time zone;
  currenttxid text;

  oldest_current_xid  text;
  autovacuum_freeze_max_age  text;
  pct_towards_emergency_autovac  text;

  tablecachehitratio numeric;
  indexcachehitratio numeric;

  temp_query_cur record;
  long_query_cur record;
  bloats_curr record;
  top_wait_eventcount_cur record;
  seq_scan_tables record;
  table_cache_hit_ratio record;
begin    
    perform pg_catalog.pg_file_unlink(g_filename);

    select 
      to_timestamp(begin_ts) , 
      to_timestamp(end_ts)  ,
      datname,
      round(100 * blks_hit / cast((blks_read + blks_hit) as numeric),2),
      temp_files,
      temp_bytes/1024/1024,
      xact_commit,
      blk_read_time/1000,
      blk_write_time/1000,
      stats_reset
      into 
      begin_time,
      end_time,
      dbname,
      dbcachehitratio,
      tempfiles,
      tempmbs,
      totalcommits,
      totalreadtimesec,
      totalwritetimesec,
      statslastreset
    from fv_stats.get_stat_database_hist(g_ts, g_interval)
    where datname = current_database();

    select pg_size_pretty(pg_database_size(current_database())) into databasesize;
    select current_setting('block_size') into databaseblocksize;
    select current_timestamp, txid_current() into currenttimestamp, currenttxid ;

    SELECT 
      age(datfrozenxid),
      current_setting('autovacuum_freeze_max_age') ,      
      ROUND(100*(age(datfrozenxid)/current_setting('autovacuum_freeze_max_age')::float))
      into 
      oldest_current_xid,
      autovacuum_freeze_max_age,
      pct_towards_emergency_autovac
    FROM pg_database 
    where datname = current_database();

    perform pg_catalog.pg_file_write(g_filename, '--------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Report Summary ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'database name: ' || dbname ||chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'begin snapshot: ' || begin_time || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'end snapshot: ' || end_time || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------- ' || chr(10) || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Database Level Usage ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Cache Hit Ratio: ' || dbcachehitratio || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Used Temporary Files: ' || tempfiles || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Used Temporary MBs: ' || tempmbs || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Commits: ' || totalcommits || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Read Time (Sec): ' || totalreadtimesec || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Write Time (sec): ' || totalwritetimesec || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Database Size (current): ' || databasesize || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Database Block Size (current): ' || databaseblocksize || chr(10) , true)  ;    
    perform pg_catalog.pg_file_write(g_filename, 'Current TXID (current): ' || currenttxid || chr(10) , true)  ;        
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;
    
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'TX Wraparound (current) ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'oldest_current_xid: ' || oldest_current_xid || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'autovacuum_freeze_max_age: ' || autovacuum_freeze_max_age || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'pct_towards_emergency_autovac: ' || pct_towards_emergency_autovac || chr(10) , true)  ; 
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 20 Wait events occured (counts) ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '---------------------' || chr(10) , true)  ;
    --
    -- header for top wait event (counts)
    --
    perform pg_catalog.pg_file_write(g_filename, format('%-50s','wait_event_type') || chr(9) || 
                                                 format('%-50s','wait_event') || chr(9) || 
                                                 'total_waits' || chr(10), true)  ;

    FOR top_wait_eventcount_cur IN
      select 
        format('%-50s',wait_event_type) as wait_event_type, 
        format('%-50s',wait_event) as wait_event, 
        --to_char(count(*), 'FM0G000') as total_waits
        count(*) as total_waits
      from fv_stats.get_stat_activity_hist(g_ts, g_interval) 
      where wait_event_type is not null
      group by wait_event_type, wait_event
      order by 3 desc
      limit 20
    LOOP
        perform pg_catalog.pg_file_write(g_filename, top_wait_eventcount_cur.wait_event_type || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, top_wait_eventcount_cur.wait_event || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, top_wait_eventcount_cur.total_waits || chr(9) , true)  ;

        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;

    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;

    --
    -- table and index cache hit ratios
    --
    SELECT 
      round(100 * sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)),2) into tablecachehitratio
    FROM 
      fv_stats.get_statio_all_tables_hist(g_ts, g_interval);
  
    SELECT 
      round(100 * sum(idx_blks_hit) / (sum(idx_blks_hit) + sum(idx_blks_read)),2) into indexcachehitratio
    FROM 
      fv_stats.get_statio_all_indexes_hist(g_ts, g_interval);  
  
    perform pg_catalog.pg_file_write(g_filename, '------------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Memory Efficiency ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '------------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Table Cache Hit Ratio: ' || tablecachehitratio || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Index Cache Hit Ratio: ' || indexcachehitratio || chr(10) || chr(10) , true)  ;
  
    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'IO Efficiency ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 20 Seq Scan Tables ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    --
    -- header for top sequencial read tables
    --
    perform pg_catalog.pg_file_write(g_filename,format('%-20s','seq_scan_ratio') || chr(9) || 
                                                format('%-50s','schemaname') || chr(9) || 
                                                format('%-50s','relname') || chr(9) || 
                                                format('%-20s','seq_scan') || chr(9) || 
                                                format('%-20s','idx_scan') || chr(9) || 
                                                format('%-20s','seq_tup_read') || chr(9) || 
                                                format('%-20s','idx_tup_fetch') || chr(10), true)  ;

    FOR seq_scan_tables IN
      select 
        100 * seq_scan / case when (idx_scan + seq_scan) = 0 then 1 else (idx_scan + seq_scan) end as seq_scan_ratio, 
        schemaname, 
        relname, 
        seq_scan, 
        idx_scan, 
        seq_tup_read, 
        idx_tup_fetch 
      from fv_stats.get_stat_all_tables_hist(g_ts, g_interval)
      where schemaname not in ('information_schema','pg_catalog','pg_toast')
        AND idx_scan IS NOT NULL
        AND seq_scan IS NOT NULL
      order by 1 desc
      limit 20
    LOOP
        perform pg_catalog.pg_file_write(g_filename, format('%-20s', seq_scan_tables.seq_scan_ratio) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-50s',seq_scan_tables.schemaname) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-50s',seq_scan_tables.relname) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',seq_scan_tables.seq_scan) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',seq_scan_tables.idx_scan) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',seq_scan_tables.seq_tup_read) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',seq_scan_tables.idx_tup_fetch) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;
    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;


    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Cache HIT Ratio for tables ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    --
    -- header for top sequencial read tables
    --
    perform pg_catalog.pg_file_write(g_filename,format('%-50s','table_name') || chr(9) || 
                                                format('%-20s','disk_hits') || chr(9) || 
                                                format('%-20s','pct_disk_hits') || chr(9) || 
                                                format('%-20s','pct_cache_hits') || chr(9) || 
                                                format('%-20s','total_hits') || chr(10), true)  ;
    FOR table_cache_hit_ratio IN
      with 
      all_tables as
      (
      SELECT  *
      FROM    (
          SELECT  'all'::text as table_name, 
              sum( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) ) as from_disk, 
              sum( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache    
          FROM    fv_stats.get_statio_all_tables_hist(g_ts, g_interval)  
          ) a
      WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
      ),
      tables as 
      (
      SELECT  *
      FROM    (
          SELECT  relname as table_name, 
              ( (coalesce(heap_blks_read,0) + coalesce(idx_blks_read,0) + coalesce(toast_blks_read,0) + coalesce(tidx_blks_read,0)) ) as from_disk, 
              ( (coalesce(heap_blks_hit,0)  + coalesce(idx_blks_hit,0)  + coalesce(toast_blks_hit,0)  + coalesce(tidx_blks_hit,0))  ) as from_cache    
          FROM    fv_stats.get_statio_all_tables_hist(g_ts, g_interval) 
          ) a
      WHERE   (from_disk + from_cache) > 0 -- discard tables without hits
      )
      SELECT  table_name as table_name,
          from_disk as disk_hits,
          round((from_disk::numeric / (from_disk + from_cache)::numeric)*100.0,2) as pct_disk_hits,
          round((from_cache::numeric / (from_disk + from_cache)::numeric)*100.0,2) as pct_cache_hits,
          (from_disk + from_cache) as total_hits
      FROM    (SELECT * FROM all_tables UNION ALL SELECT * FROM tables) a
      ORDER   BY (case when table_name = 'all' then 0 else 1 end), pct_disk_hits desc
      limit 20
      LOOP
        perform pg_catalog.pg_file_write(g_filename, format('%-50s',table_cache_hit_ratio.table_name) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',table_cache_hit_ratio.disk_hits) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',table_cache_hit_ratio.pct_disk_hits) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',table_cache_hit_ratio.pct_cache_hits) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',table_cache_hit_ratio.total_hits) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;
    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Temp Usage ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 10 Temp Usage By Queries  ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, format('%-20s','total_exec_time') || chr(9) || 
                                                 format('%-20s','ncalls') || chr(9) || 
                                                 format('%-20s','avg_exec_time_sec') || chr(9) || 
                                                 format('%-20s','sync_io_time') || chr(9) || 
                                                 format('%-20s','temp_blks_written') || chr(9) ||
                                                 format('%-20s','queryid') || chr(9) || 
                                                 'query' || chr(10), true)  ;

    FOR temp_query_cur IN
        SELECT
          INTERVAL '1 millisecond' * total_time AS total_exec_time,
          to_char(calls, 'FM999G999G999G990') AS ncalls,
          to_char((total_time / calls) / 1000, 'FM999G999G990.999') AS avg_exec_time_sec,
          INTERVAL '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time,
          temp_blks_written,
          temp_blks_written,
          queryid as queryid,
          --substring(replace(query,chr(10),' '),0,200) AS query
          substring(replace(query,chr(10),' '),0,200) AS query
        FROM
          fv_stats.get_stat_statements_hist(g_ts, g_interval)    
        WHERE
           temp_blks_written > 0
        ORDER BY
            temp_blks_written DESC
        LIMIT 10
    LOOP
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.total_exec_time) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.ncalls) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.avg_exec_time_sec) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.sync_io_time) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.temp_blks_written) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',temp_query_cur.queryid) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, temp_query_cur.query || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;
    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 20 Long Running Queries  ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, format('%-20s','ms_per_execution') || chr(9) || 
                                                 format('%-20s','ncalls') || chr(9) || 
                                                 format('%-20s','total_exec_time') || chr(9) || 
                                                 format('%-20s','mean_time') || chr(9) || 
                                                 format('%-20s','rrows') || chr(9) ||
                                                 format('%-20s','shared_blks_hit') || chr(9) ||
                                                 format('%-20s','shared_blks_read') || chr(9) ||
                                                 format('%-20s','local_blks_hit') || chr(9) ||
                                                 format('%-20s','local_blks_read') || chr(9) ||
                                                 format('%-20s','temp_blks_read') || chr(9) ||
                                                 format('%-20s','temp_blks_written') || chr(9) ||
                                                 format('%-20s','blk_read_time') || chr(9) ||
                                                 format('%-20s','blk_write_time') || chr(9) ||
                                                 format('%-20s','userid') || chr(9) ||
                                                 format('%-20s','queryid') || chr(9) || 
                                                 'query' || chr(10), true)  ;

    FOR long_query_cur IN 
        select 
          (INTERVAL '1 millisecond' * total_time) / calls as ms_per_execution, 
          to_char(calls, 'FM999G999G999G990') AS ncalls,
          INTERVAL '1 millisecond' * total_time AS total_exec_time, 
          mean_time, 
          rows as rrows, 
          shared_blks_hit, shared_blks_read, 
          local_blks_hit, local_blks_read, 
          temp_blks_read, temp_blks_written, 
          blk_read_time, blk_write_time, userid, queryid,
          substring(replace(query,chr(10),' '),0,200) AS query
        from fv_stats.get_stat_statements_hist(g_ts, g_interval) 
        order by 1 desc
        limit 20
    LOOP
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.ms_per_execution) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.ncalls) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.total_exec_time) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.mean_time) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.rrows) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.shared_blks_hit) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.shared_blks_read) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.local_blks_hit) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.local_blks_read) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.temp_blks_read) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.temp_blks_written) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.blk_read_time) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.blk_write_time) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.userid) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',long_query_cur.queryid) || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, long_query_cur.query || chr(9) , true)  ;
        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;
    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;



    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 20 Bloat Usage By Tables (current)  ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, format('%-30s','current_database') || chr(9) || 
                                                 format('%-30s','schema_name') || chr(9) || 
                                                 format('%-30s','table_name') || chr(9) || 
                                                 format('%-20s','tbloat') || chr(9) || 
                                                 format('%-20s','wasted_bytes') || chr(9) ||
                                                 format('%-20s','ibloat') || chr(9) ||
                                                 format('%-20s','wastedibytes') || chr(10), true)  ;

    FOR bloats_curr IN 
        SELECT
          current_database() as current_database, schemaname, tablename, /*reltuples::bigint, relpages::bigint, otta,*/
          ROUND((CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages::FLOAT/otta END)::NUMERIC,1) AS tbloat,
          CASE WHEN relpages < otta THEN 0 ELSE bs*(sml.relpages-otta)::BIGINT END AS wastedbytes,
          iname, /*ituples::bigint, ipages::bigint, iotta,*/
          ROUND((CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages::FLOAT/iotta END)::NUMERIC,1) AS ibloat,
          CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes
        FROM (
          SELECT
            schemaname, tablename, cc.reltuples, cc.relpages, bs,
            CEIL((cc.reltuples*((datahdr+ma-
              (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::FLOAT)) AS otta,
            COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
            COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::FLOAT)),0) AS iotta -- very rough approximation, assumes all cols
          FROM (
            SELECT
              ma,bs,schemaname,tablename,
              (datawidth+(hdr+ma-(CASE WHEN hdr%ma=0 THEN ma ELSE hdr%ma END)))::NUMERIC AS datahdr,
              (maxfracsum*(nullhdr+ma-(CASE WHEN nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
            FROM (
              SELECT
                schemaname, tablename, hdr, ma, bs,
                SUM((1-null_frac)*avg_width) AS datawidth,
                MAX(null_frac) AS maxfracsum,
                hdr+(
                  SELECT 1+COUNT(*)/8
                  FROM pg_stats s2
                  WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
              FROM pg_stats s, (
                SELECT
                  (SELECT current_setting('block_size')::NUMERIC) AS bs,
                  CASE WHEN SUBSTRING(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                  CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
                FROM (SELECT version() AS v) AS foo
              ) AS constants
              GROUP BY 1,2,3,4,5
            ) AS foo
          ) AS rs
          JOIN pg_class cc ON cc.relname = rs.tablename
          JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname AND nn.nspname <> 'information_schema'
          LEFT JOIN pg_index i ON indrelid = cc.oid
          LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
        ) AS sml
        ORDER BY wastedbytes desc
        limit 20
    LOOP

        perform pg_catalog.pg_file_write(g_filename, format('%-30s',bloats_curr.current_database) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-30s',bloats_curr.schemaname) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-30s',bloats_curr.tablename) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',bloats_curr.tbloat) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',bloats_curr.wastedbytes) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',bloats_curr.ibloat) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, format('%-20s',bloats_curr.wastedibytes) || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, chr(10) , true)  ;
    END LOOP;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;

END;
$$
LANGUAGE plpgsql

call fv_stats.generate_report (cast(extract (epoch from now()) as bigint), INTERVAL '5 days', 'awr.txt');

  




