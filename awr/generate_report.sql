DROP FUNCTION IF EXISTS fv_stats.generate_report(bigint, interval);
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
  
  tablecachehitratio numeric;
  indexcachehitratio numeric;

  temp_query_cur record;
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
      blk_write_time/1000
      into 
      begin_time,
      end_time,
      dbname,
      dbcachehitratio,
      tempfiles,
      tempmbs,
      totalcommits,
      totalreadtimesec,
      totalwritetimesec
    from fv_stats.get_stat_database_hist(g_ts, g_interval)
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
    perform pg_catalog.pg_file_write(g_filename, '---------------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Cache Hit Ratio: ' || dbcachehitratio || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Used Temporary Files: ' || tempfiles || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Used Temporary MBs: ' || tempmbs || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Commits: ' || totalcommits || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Read Time (Sec): ' || totalreadtimesec || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Total Write Time (sec): ' || totalwritetimesec || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-------------------- ' || chr(10) || chr(10) , true)  ;
    
    SELECT 
      round(100 * sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)),2) into tablecachehitratio
    FROM 
      fv_stats.get_statio_all_tables_hist(g_ts, g_interval);
  
    SELECT 
      round(100 * sum(idx_blks_hit) / (sum(idx_blks_hit) + sum(idx_blks_read)),2) into indexcachehitratio
    FROM 
      fv_stats.get_statio_all_indexes_hist(cast(extract (epoch from now()) as bigint), INTERVAL '1 hour');  
  
    perform pg_catalog.pg_file_write(g_filename, '------------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Memory Efficiency ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '------------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Table Cache Hit Ratio: ' || tablecachehitratio || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Index Cache Hit Ratio: ' || indexcachehitratio || chr(10) || chr(10) , true)  ;
  

    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'IO Efficiency ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '--------------' || chr(10) , true)  ;


    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Temp Usage ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, '---------- ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, 'Top 10 Temp Usage By Queries  ' || chr(10) , true)  ;
    perform pg_catalog.pg_file_write(g_filename, '-----------' || chr(10) , true)  ;

    perform pg_catalog.pg_file_write(g_filename, 'total_exec_time' || chr(9) || 
                                                 'ncalls' || chr(9) || 
                                                 'avg_exec_time_sec' || chr(9) || 
                                                 'sync_io_time' || chr(9) || 
                                                 'temp_blks_written' || chr(9) ||
                                                 'queryid' || chr(9) || 
                                                 'query' || chr(10), true)  ;

    FOR temp_query_cur IN

        SELECT
          INTERVAL '1 millisecond' * total_time AS total_exec_time,
          to_char(calls, 'FM999G999G999G990') AS ncalls,
          (total_time / calls) / 1000 AS avg_exec_time_sec,
          INTERVAL '1 millisecond' * (blk_read_time + blk_write_time) AS sync_io_time,
          temp_blks_written,
          queryid as queryid,
          query AS query
        FROM
          fv_stats.get_stat_statements_hist(g_ts, g_interval)    
        WHERE
           temp_blks_written > 0
        ORDER BY
            temp_blks_written DESC
        LIMIT 10
    LOOP

        perform pg_catalog.pg_file_write(g_filename, temp_query_cur.total_exec_time || chr(9), true)  ;
        perform pg_catalog.pg_file_write(g_filename, temp_query_cur.ncalls || chr(9) , true)  ;

    END LOOP;




END;
$$
LANGUAGE plpgsql


call fv_stats.generate_report (cast(extract (epoch from now()) as bigint), INTERVAL '1 hour', 'awr.txt');

  
  
  



