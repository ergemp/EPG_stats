create or replace procedure fv_stats.drop_meta()  as
$$
declare 
begin 
	execute 'drop table if exists fv_stats.stat_all_tables_hist';
	execute 'drop table if exists fv_stats.stat_all_indexes_hist';
	execute 'drop table if exists fv_stats.statio_all_tables_hist';
	execute 'drop table if exists fv_stats.statio_all_indexes_hist';
	execute 'drop table if exists fv_stats.stat_activity_hist';
	execute 'drop table if exists fv_stats.stat_statements_hist';
	execute 'drop table if exists fv_stats.stat_archiver_hist';
	execute 'drop table if exists fv_stats.stat_bgwriter_hist';
	execute 'drop table if exists fv_stats.stat_locks_hist';
	execute 'drop table if exists fv_stats.stat_database_hist';
end
$$
language plpgsql 