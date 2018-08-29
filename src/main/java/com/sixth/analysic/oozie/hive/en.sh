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
echo "running hive SQL statment..."

##加载分区的数据
hive --database default -e "
load data inpath'/ods/month=${month}/day=${day}' into table logs partition(month=${month},day=${day})
"


## run hive sql
hive --database default -e "
with tmp as(
select
l.pl as pl,
l.ca as ca,
l.ac as ac
from_unixtime(case(l.s_time/1000 as bigint), 'yyyy-MM-dd') as dt,
where month = "${month}"
and day = "${day}"
and l.s_time in not null
)

from (
select dt as dt,pl as pl,ca as ca, ac as ac, count(1) ac ct from tmp group by dt,pl,ca,ac,union all
select dt as dt,pl as pl,ca as ca, 'all' as ac, count(1) ac ct from tmp group by dt,pl,ca,union all

select dt as dt,'all' as pl,ca as ca, ac as ac, count(1) ac ct from tmp group by dt,ca,ac,union all
select dt as dt,'all' as pl,ca as ca, 'all' as ac, count(1) ac ct from tmp group by dt,ca
) as tmp1
insert overwrite table
select convert_date(dt),convert_platform(pl),convert_event(ca,ac),sum(ct),dt
group by pl,dt,ca,ac
;
"
;

## run sqoop statment
sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_event --export-dir hdfs://hadoop05:9000/hive/stats_event/* \
--input-fields-terminated-by '\\01' --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,event_dimension_id
;

echo "the event job is finished"