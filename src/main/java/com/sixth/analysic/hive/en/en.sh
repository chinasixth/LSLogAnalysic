#!/bin/bash

####命令行输入： ./en.sh -d 2018-08-20   如果-d后面没有参数运行昨天的数据


dt=''
##循环运行时所带的参数
##  $#:参数的个数
until [ $# -eq 0 ]
do
if[ $1'x' = '-dx' ]
then
## shift：去掉第一个参数
shift
dt=$1
fi
shift
done

month=''
day=''
##判断日期是否合法和正常
##先判断时间的长度
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
with tmp as(
select
from_unixtime(cast(l.s_time/1000 as bigint),'yyyy-MM-dd') as dt,
l.pl as pl,
l.ca as ca,
l.ac as ac
from logs l
where month = 8
and day = 20
and l.s_time <> 'null'
)
from (
select dt as dt,pl as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,pl,ca,ac union all
select dt as dt,pl as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,pl,ca union all
select dt as dt,'all' as pl,ca as ca,ac as ac,count(1) as ct from tmp group by dt,ca,ac union all
select dt as dt,'all' as pl,ca as ca,'all' as ac,count(1) as ct from tmp group by dt,ca
) as tmp1
insert overwrite table stats_event
select convert_platform(pl),convert_date(dt),convert_event(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;
"


## run sqoop statment
sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_event --export-dir 'hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_event/*' \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,event_dimension_id


echo "the event job is finished"