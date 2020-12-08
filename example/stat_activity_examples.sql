select * from fv_stats.get_stat_activity_hist(cast(extract( epoch from now()) as bigint), interval '5 days');

select 
  max(ts), 
  to_timestamp(max(ts)),
  min(ts), 
  to_timestamp(min(ts)) 
from fv_stats.get_stat_activity_hist(cast(extract( epoch from now()) as bigint), interval '5 days');
--1	1607463000	2020-12-08T21:30:00.000Z	1607462101	2020-12-08T21:15:01.000Z

select 
  max(ts), 
  to_timestamp(max(ts)),
  min(ts), 
  to_timestamp(min(ts)) 
from fv_stats.stat_activity_hist;
--1	1607463000	2020-12-08T21:30:00.000Z	1607346900	2020-12-07T13:15:00.000Z