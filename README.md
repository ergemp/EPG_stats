# epg_stat_history
Statistics repository and interval based performance reports with plpgsql

# Description
EPG stat history collects the statistics from the Postgresql catalog and creates its own repository. Then simply select the statistical information within the supplied time period. 

With EPG stat history, you are able to ask questions like;
Whats is my IO for the last hour.
What was the performance bottleneck yesterday night between 23:00 and 03:00 hours. 
And so on... 

# Installation Prerequisities
## Configuring statistics collector
EPG stats is highly dependent to the Postgresql native statistics collector. So at least the following tracking options should be enabled.

You can configure the settings by editing the postgresql.conf file.

```
track_counts = on
track_io_timing = on
track_activities = on
track_activity_query_size = 1024
```

Or, you can alter the system.

```
alter system set track_counts = on;
alter system set track_io_timing = on;
alter system set track_activities = on;
alter system set track_activity_query_size = 1024;
```

## Installing pg_stat_statements
EPG stats also collects the pg_stat_statements data. So installting the extension is a must. 

```
create extension pg_stat_statements; 
select * from pg_extensions;
select * from pg_stat_statements; 
```
## Installing adminpack
In order to be able to create text file based reports adminpack extension must be installed. 

```
create extension adminpack;
```

## EPG_stats installation
Download the codes from this site. (https://github.com/ergemp/FV_stats/)
Copy the files to your postgresql database.
Unzip the files and cd to the unzipped folder. 

You should get the following dircetory structure
```
ergemp@Ergems-MacBook-Pro fv_stats % ls -l
total 8
-rw-r--r--   1 ergemp  staff   85 Dec  7 10:25 README.md
drwxr-xr-x   3 ergemp  staff   96 Dec  7 16:18 awr
drwxr-xr-x   6 ergemp  staff  192 Dec 10 11:50 example
drwxr-xr-x  12 ergemp  staff  384 Dec  7 09:52 getter
drwxr-xr-x   4 ergemp  staff  128 Dec  7 11:53 install
drwxr-xr-x   8 ergemp  staff  256 Dec  7 12:26 metadata
drwxr-xr-x   5 ergemp  staff  160 Dec  7 09:54 util
```

Without changing the current directory execute install/install.sh <your_database_name>

This script will create a schema named fv_stats and install its own repository there. 
Do not forget to supply the name of your database. FV_stats ONLY works on the database it is installed in. 

## Patching FV_stats with a new version
After downloading and uncompressing the FV_stats from the github page, instead of installing from scratch you can just install the functions and keep you collected repository. To do this, run install/patch.sh instead of the installer. 

```
install/patch.sh <your_database_name>
```


# Examples 

## Filling up the repo
In order to start filling up the repository run the ```call fv_stats.fill_meta();``` command. The more frequently you run, more granular information you can get, but this also means more data to gather and ends up space consumption. Every 30 minutes for filling the repository is enough for most of the cases. 

## Gathering the performance report
To generate a report run the fv_stats.generate_report procedure with the required parameters. 

The following command will generate an overview report of the postgresql database for the last one day. The awr.txt file should be created in the $PGDATA directory. 

```
call fv_stats.generate_report (cast(extract (epoch from now()) as bigint), INTERVAL '1 days', 'awr.txt');
```

Sample report can be found in the "sample" directory. 










