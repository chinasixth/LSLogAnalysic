#!/bin/bash

dt=''
until [ $# -eq 0 ]
do
if[ $1'x' = '-dx' ]
then
shift
dt=$1
fi
shift
done

month=''
day=''

if [ $[#dt] = 10 ]
then
echo "dt:$dt"
else
dt=`date -d "1 day age" "+%y-%m-d%"`
month=`date -d "$dt" "+%m"`
day=`date -d "$dt" "+%d"`
echo "running date is $dt, month is $month, day is $day"
echo "runnint hive SQL statment..."

## run hive sql
hive --database default -e "
from(
select
from_unixtime(case(l.s_time/1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.u_ud as uid,
(case
when count(l.p_url) = 1 then 'pv1'
when count(l.p_url) = 2 then 'pv2'
when count(l.p_url) = 3 then 'pv3'
when count(l.p_url) = 4 then 'pv4'
when count(l.p_url) < 10 then 'pv5_10'
when count(l.p_url) < 30 then 'pv10_30'
when count(l.p_url) < 60 then 'pv30_60'
else 'pv6pluss'
end
) as pv
from logs l
where month = 08
and day = 20
and l.p_url <> 'null'
and l.pl is not null
group by from_unixtime(case(l.s_time/1000 as bigint), 'yyyy-MM-dd'),l.pl,l.u_ud,
) as tmp
insert overwrite table stats_view_depth_tmp
select dt,pl,pv,count(distinct(uid)) as ct
where uid is not null
group by (dt,pl,pv)
;
"
;

hive --database default -e "
with tmp as ()
select
dt,pl as pl, ct as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv1' union all
dt,pl as pl, 0 as pv1, ct as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv2' union all
dt,pl as pl, 0 as pv1, 0 as pv2, ct as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv3' union all
dt,pl as pl, 0 as pv1, 0 as pv2, 0 as pv3, ct as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv4' union all
dt,pl as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, ct as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv5_10' union all
dt,pl as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, ct as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv10_30' union all
dt,pl as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, ct as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv30_60' union all
dt,pl as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, ct as pv60pluss from stats_view_depth_tmp where ct = 'pv60pluss' union all

dt,'all' as pl, ct as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv1' union all
dt,'all' as pl, 0 as pv1, ct as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv2' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, ct as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv3' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, 0 as pv3, ct as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv4' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, ct as pv5_10, 0 as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv5_10' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, ct as pv10_30, 0 as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv10_30' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, ct as pv_30_60, 0 as pv60pluss from stats_view_depth_tmp where ct = 'pv30_60' union all
dt,'all' as pl, 0 as pv1, 0 as pv2, 0 as pv3, 0 as pv4, 0 as pv5_10, 0 as pv10_30, 0 as pv_30_60, ct as pv60pluss from stats_view_depth_tmp where ct = 'pv60pluss' union all
)
from tmp
insert overwrite table stats_view_depth
select convert_date(dt),convert_platform(pl),3,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by (dt, pl)
;
"
;

sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_view_depth --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_view_depth/* \
--input-fields-terminated-by '\\01' --update-mode allowinsert \
--update-key   `platform_dimension_id`,`data_dimension_id`, `kpi_dimension_id`
;

echo "the pv job is finished"