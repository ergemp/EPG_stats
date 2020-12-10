# FV_stats
Statistics repository and interval based performance reports with plpgsql

# Description
FV stats collects the statistics from the Postgresql catalog and creates its own repository. Then simply select the statistical information within the supplied time period. 

With FV stats, you are able to ask questions like;
Whats is my IO for the last hour.
What was the performance bottleneck yesterday night between 23:00 and 03:00 hours. 
And so on... 

# Installation
## Configuring statistics collector
FV stats is highly dependent to the Postgresql native statistics collector. So at least the following tracking options should be enabled.

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





