CREATE OR replace procedure fv_stats.fill_meta()  as
$$
DECLARE
  currentts bigint;
BEGIN
	
	currentts := extract(epoch from now()) ;

	execute 'INSERT INTO fv_stats.stat_all_tables_hist 
				(
				  ts ,
				  relid ,
				  schemaname ,
				  relname ,
				  seq_scan ,
				  seq_tup_read ,
				  idx_scan , 
				  idx_tup_fetch , 
				  n_tup_ins , 
				  n_tup_upd , 
				  n_tup_del , 
				  n_tup_hot_upd , 
				  n_live_tup , 
				  n_dead_tup , 
				  n_mod_since_analyze , 
				  last_vacuum , 
				  last_autovacuum ,
				  last_analyze ,
				  last_autoanalyze ,
				  vacuum_count , 
				  autovacuum_count , 
				  analyze_count , 
				  autoanalyze_count 				  
				)
				SELECT 
				  ' || currentts || ',
				  relid ,
				  schemaname ,
				  relname ,
				  seq_scan ,
				  seq_tup_read ,
				  idx_scan , 
				  idx_tup_fetch , 
				  n_tup_ins , 
				  n_tup_upd , 
				  n_tup_del , 
				  n_tup_hot_upd , 
				  n_live_tup , 
				  n_dead_tup , 
				  n_mod_since_analyze , 
				  last_vacuum , 
				  last_autovacuum ,
				  last_analyze ,
				  last_autoanalyze ,
				  vacuum_count , 
				  autovacuum_count , 
				  analyze_count , 
				  autoanalyze_count 
				FROM pg_stat_all_tables';
			
	execute 'INSERT INTO fv_stats.stat_all_indexes_hist 
				(				  
				  ts ,
				  relid ,
				  indexrelid ,
				  schemaname ,
				  relname ,
				  indexrelname ,
				  idx_scan ,
				  idx_tup_read , 
				  idx_tup_fetch 
				)
				SELECT 
				  ' || currentts || ' ,
				  relid ,
				  indexrelid ,
				  schemaname ,
				  relname ,
				  indexrelname ,
				  idx_scan ,
				  idx_tup_read , 
				  idx_tup_fetch 
				FROM pg_stat_all_indexes';		

	execute 'INSERT INTO fv_stats.statio_all_tables_hist
				(
					ts , 
					relid , 
					schemaname ,
					relname ,
					heap_blks_read , 
					heap_blks_hit ,
					idx_blks_read , 
					idx_blks_hit , 
					toast_blks_read , 
					toast_blks_hit ,
					tidx_blks_read , 
					tidx_blks_hit 
				)
				SELECT 
					' ||currentts || ' , 
					relid , 
					schemaname ,
					relname ,
					heap_blks_read , 
					heap_blks_hit ,
					idx_blks_read , 
					idx_blks_hit , 
					toast_blks_read , 
					toast_blks_hit ,
					tidx_blks_read , 
					tidx_blks_hit 
				FROM pg_statio_all_tables';
			
	execute 'INSERT INTO fv_stats.statio_all_indexes_hist
				(
					ts , 
					relid ,
					indexrelid ,
					schemaname ,
					relname ,
					indexrelname ,
					idx_blks_read , 
					idx_blks_hit 
				)
				SELECT 
					' || currentts || ' , 
					relid ,
					indexrelid ,
					schemaname ,
					relname ,
					indexrelname ,
					idx_blks_read , 
					idx_blks_hit  
					FROM pg_statio_all_indexes';
				
	execute 'INSERT INTO fv_stats.stat_activity_hist 
				(
				  ts,
				  datid,
				  datname,
				  pid,
				  usesysid,
				  usename,
				  application_name,
				  client_addr,
				  client_hostname,
				  client_port,
				  backend_start,
				  xact_start,
				  query_start,
				  state_change,
				  wait_event_type,
				  wait_event,
				  state,
				  backend_xid,
				  backend_xmin,
				  query,
				  backend_type
				)
				SELECT 				 
				  ' || currentts|| ',
				  datid,
				  datname,
				  pid,
				  usesysid,
				  usename,
				  application_name,
				  client_addr,
				  client_hostname,
				  client_port,
				  backend_start,
				  xact_start,
				  query_start,
				  state_change,
				  wait_event_type,
				  wait_event,
				  state,
				  backend_xid,
				  backend_xmin,
				  query,
				  backend_type
				FROM pg_stat_activity';	
			
	execute 'INSERT INTO fv_stats.stat_archiver_hist
				(
					ts , 
					archived_count , 
					last_archived_wal ,
					last_archived_time ,
					failed_count , 
					last_failed_wal ,
					last_failed_time , 
					stats_reset 
				)
				SELECT 
					' || currentts || ' ,
					archived_count , 
					last_archived_wal ,
					last_archived_time ,
					failed_count , 
					last_failed_wal ,
					last_failed_time , 
					stats_reset 
				FROM pg_stat_archiver';
			
	execute 'INSERT INTO fv_stats.stat_bgwriter_hist
				(
					ts ,
					checkpoints_timed , 
					checkpoints_req , 
					checkpoint_write_time ,
					checkpoint_sync_time ,
					buffers_checkpoint ,
					buffers_clean , 
					maxwritten_clean , 
					buffers_backend , 
					buffers_backend_fsync , 
					buffers_alloc , 
					stats_reset 
				)
				SELECT 
					' || currentts || ',
					checkpoints_timed , 
					checkpoints_req , 
					checkpoint_write_time ,
					checkpoint_sync_time ,
					buffers_checkpoint ,
					buffers_clean , 
					maxwritten_clean , 
					buffers_backend , 
					buffers_backend_fsync , 
					buffers_alloc , 
					stats_reset 
				FROM pg_stat_bgwriter';
			
	execute 'INSERT INTO fv_stats.stat_statements_hist 
				(
					ts ,
					userid ,
					dbid ,
					queryid ,
					query ,
					calls ,
					total_time ,
					min_time ,
					max_time ,
					mean_time ,
					stddev_time ,
					rows ,
					shared_blks_hit ,
					shared_blks_read ,
					shared_blks_dirtied ,
					shared_blks_written ,
					local_blks_hit ,
					local_blks_read ,
					local_blks_dirtied ,
					local_blks_written ,
					temp_blks_read ,
					temp_blks_written ,
					blk_read_time ,
					blk_write_time 
				)
				SELECT 
					' || currentts || ',
					userid ,
					dbid ,
					queryid ,
					query ,
					calls ,
					total_time ,
					min_time ,
					max_time ,
					mean_time ,
					stddev_time ,
					rows ,
					shared_blks_hit ,
					shared_blks_read ,
					shared_blks_dirtied ,
					shared_blks_written ,
					local_blks_hit ,
					local_blks_read ,
					local_blks_dirtied ,
					local_blks_written ,
					temp_blks_read ,
					temp_blks_written ,
					blk_read_time ,
					blk_write_time 
				FROM pg_stat_statements';			

	execute 'INSERT INTO fv_Stats.stat_locks_hist 
				(
					ts ,
					locktype ,
					database , 
					relation ,
					page ,
					tuple ,
					virtualxid , 
					transactionid , 
					classid ,
					objid ,
					objsubid ,
					virtualtransaction , 
					pid , 
					mode , 
					granted , 
					fastpath 
				)
				SELECT 
					' || currentts || ' ,
					locktype ,
					database , 
					relation ,
					page ,
					tuple ,
					virtualxid , 
					transactionid , 
					classid ,
					objid ,
					objsubid ,
					virtualtransaction , 
					pid , 
					mode , 
					granted , 
					fastpath 
				FROM pg_locks';

	execute 'INSERT INTO fv_stats.stat_database_hist 
				(
					ts ,
					datid ,
					datname	,
					numbackends ,
					xact_commit ,
					xact_rollback ,
					blks_read ,
					blks_hit ,
					tup_returned ,
					tup_fetched ,
					tup_inserted ,
					tup_updated ,
					tup_deleted ,
					conflicts ,
					temp_files ,
					temp_bytes ,
					deadlocks ,
					blk_read_time ,
					blk_write_time ,
					stats_reset 
				)
				SELECT 
					'|| currentts || ' ,
					datid ,
					datname	,
					numbackends ,
					xact_commit ,
					xact_rollback ,
					blks_read ,
					blks_hit ,
					tup_returned ,
					tup_fetched ,
					tup_inserted ,
					tup_updated ,
					tup_deleted ,
					conflicts ,
					temp_files ,
					temp_bytes ,
					deadlocks ,
					blk_read_time ,
					blk_write_time ,
					stats_reset 
				FROM pg_stat_database';			
			
END
$$
LANGUAGE plpgsql


--call fv_stats.fill_meta();


