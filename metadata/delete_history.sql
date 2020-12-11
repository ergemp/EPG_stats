drop procedure if exists fv_stats.delete_history(interval);

create or replace procedure fv_stats.delete_history(g_interval interval)  as
$$
declare 
  nnow timestamp with time zone := now()::timestamp with time zone;
begin 	    
    delete from fv_stats.stat_activity_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_all_indexes_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_all_tables_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.statio_all_tables_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.statio_all_indexes_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_statements_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_locks_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_database_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_bgwriter_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
    delete from fv_stats.stat_archiver_hist where ts < cast(extract (epoch from nnow - g_interval) as bigint);
end
$$
language plpgsql 

--call fv_stats.delete_history(interval '10 minute' );