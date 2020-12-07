CREATE OR replace procedure fv_stats.create_meta()  as
$$
DECLARE

BEGIN

	EXECUTE 'create table if not exists fv_stats.stat_all_tables_hist
				(
				  ts bigint,
				  relid oid,
				  schemaname varchar(100),
				  relname varchar(100),
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
				)';			
	EXECUTE 'create index if not exists ix_stat_all_tables_hist on fv_stats.stat_all_tables_hist(ts)';

    EXECUTE 'create table if not exists fv_stats.stat_all_indexes_hist
				(
				  ts bigint,
				  relid oid,
				  indexrelid oid,
				  schemaname varchar(100),
				  relname varchar(100),
				  indexrelname varchar(100),
				  idx_scan bigint,
				  idx_tup_read bigint, 
				  idx_tup_fetch bigint
				)';			
	EXECUTE 'create index if not exists ix_stat_all_indexes_hist on fv_stats.stat_all_indexes_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.statio_all_tables_hist 
				(
					ts bigint, 
					relid oid, 
					schemaname varchar(100),
					relname varchar(100),
					heap_blks_read bigint, 
					heap_blks_hit bigint,
					idx_blks_read bigint, 
					idx_blks_hit bigint, 
					toast_blks_read bigint, 
					toast_blks_hit bigint,
					tidx_blks_read bigint, 
					tidx_blks_hit bigint
			 	)';
	EXECUTE 'create index if not exists ix_statio_all_tables_hist on fv_stats.statio_all_tables_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.statio_all_indexes_hist 
				(
					ts bigint, 
					relid oid, 
					indexrelid oid,
					schemaname varchar(100),
					relname varchar(100),
					indexrelname varchar(100),
					idx_blks_read bigint, 
					idx_blks_hit bigint
			 	)';
	EXECUTE 'create index if not exists ix_statio_all_indexes_hist on fv_stats.statio_all_indexes_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_activity_hist 
				(
					ts bigint, 
					datid oid, 
					datname varchar(100),
					pid integer,
					usesysid oid, 
					usename varchar(100),
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
			 	)';
	EXECUTE 'create index if not exists ix_stat_activity_hist on fv_stats.stat_activity_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_archiver_hist 
				(
					ts bigint, 
					archived_count bigint, 
					last_archived_wal text,
					last_archived_time timestamp with time zone,
					failed_count bigint, 
					last_failed_wal text,
					last_failed_time timestamp with time zone, 
					stats_reset timestamp with time zone
			 	)';
	EXECUTE 'create index if not exists ix_stat_archiver_hist on fv_stats.stat_archiver_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_bgwriter_hist 
				(
					ts bigint,
					checkpoints_timed bigint, 
					checkpoints_req bigint, 
					checkpoint_write_time double precision,
					checkpoint_sync_time double precision,
					buffers_checkpoint bigint,
					buffers_clean bigint, 
					maxwritten_clean bigint, 
					buffers_backend bigint, 
					buffers_backend_fsync bigint, 
					buffers_alloc bigint, 
					stats_reset timestamp with time zone 
			 	)';
	EXECUTE 'create index if not exists ix_stat_bgwriter_hist on fv_stats.stat_bgwriter_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_statements_hist 
				(
					ts bigint,
					userid oid,
					dbid oid,
					queryid bigint,
					query text,
					calls bigint,
					total_time double precision,
					min_time double precision,
					max_time double precision,
					mean_time double precision,
					stddev_time double precision,
					rows bigint,
					shared_blks_hit bigint,
					shared_blks_read bigint,
					shared_blks_dirtied bigint,
					shared_blks_written bigint,
					local_blks_hit bigint,
					local_blks_read bigint,
					local_blks_dirtied bigint,
					local_blks_written bigint,
					temp_blks_read bigint,
					temp_blks_written bigint,
					blk_read_time double precision,
					blk_write_time double precision					
			 	)';
	EXECUTE 'create index if not exists ix_stat_statements_hist on fv_stats.stat_statements_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_locks_hist 
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
			 	)';
	EXECUTE 'create index if not exists ix_stat_locks_hist on fv_stats.stat_locks_hist(ts)';

	EXECUTE 'create table if not exists fv_stats.stat_database_hist 
				(
					ts bigint,
					datid oid,
					datname	varchar(100),
					numbackends integer,
					xact_commit bigint,
					xact_rollback bigint,
					blks_read bigint,
					blks_hit bigint,
					tup_returned bigint,
					tup_fetched bigint,
					tup_inserted bigint,
					tup_updated bigint,
					tup_deleted bigint,
					conflicts bigint,
					temp_files bigint,
					temp_bytes bigint,
					deadlocks bigint,
					checksum_failures bigint,
					checksum_last_failure timestamp with time zone,
					blk_read_time double precision,
					blk_write_time double precision,
					stats_reset timestamp with time zone
			 	)';
	EXECUTE 'create index if not exists ix_stat_database_hist on fv_stats.stat_database_hist(ts)';

END;
$$
language plpgsql

call fv_stats.drop_meta();
call fv_stats.create_meta();

