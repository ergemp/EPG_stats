if [ $# -eq 0 ]
  then
    echo "No arguments supplied"
    exit -1
fi

if [ -z "$1" ]
  then
    echo "First argument should be supplied as database name"
fi

# install meta functions
psql -d $1 -f metadata/create_meta.sql
psql -d $1 -f metadata/drop_meta.sql
psql -d $1 -f metadata/fill_meta.sql
psql -d $1 -f metadata/delete_history.sql

# install util functions
psql -d $1 -f util/check_last_ts.sql
psql -d $1 -f util/find_between.sql
psql -d $1 -f util/find_interval.sql

# install getter functions 
psql -d $1 -f getter/get_stat_activity_hist.sql
psql -d $1 -f getter/get_stat_all_indexes_hist.sql
psql -d $1 -f getter/get_stat_all_tables_hist.sql
psql -d $1 -f getter/get_statio_all_indexes_hist.sql
psql -d $1 -f getter/get_statio_all_tables_hist.sql
psql -d $1 -f getter/get_stat_archiver_hist.sql
psql -d $1 -f getter/get_stat_bgwriter_hist.sql
psql -d $1 -f getter/get_stat_database_hist.sql
psql -d $1 -f getter/get_stat_locks_hist.sql
psql -d $1 -f getter/get_stat_statements_hist.sql

# install awr 
psql -d $1 -f awr/generate_report.sql
