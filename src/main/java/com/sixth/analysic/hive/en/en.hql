create function convert_date as 'com.sixth.analysic.hive.DateDimensionUdf' using jar '/lsloganalysic/udf/jars/LSLogAnalysic-1.0.jar';
create function convert_platform as 'com.sixth.analysic.hive.PlatformDimensionUdf' using jar '/lsloganalysic/udf/jars/LSLogAnalysic-1.0.jar';
create function convert_event as 'com.sixth.analysic.hive.EventDimensionUdf' using jar '/lsloganalysic/udf/jars/LSLogAnalysic-1.0.jar';

select
 l.s_time,
 l.pl,
 l.ca,
 l.ac
 from logs l
 ;

结果表:
CREATE TABLE IF NOT EXISTS `stats_event` (
  `date_dimension_id` int,
  `platform_dimension_id` int,
  `event_dimension_id` int,
  `times` int,
  `created` string
);



将hive查询的结果放到hive上的一张表中，然后使用sqoop导入到mysql中
// 将结果存到内存中，作为一张临时表
with tmp as(
select
l.pl as pl,
l.ca as ca,
l.ac as ac
from_unixtime(cast(l.s_time/1000 as bigint), "yyyy-MM-dd") as dt,
where month = 8
and day = 17
and l.s_time in not null  // <>后面跟的是字符串，如：<> 'null'
)

from (
select dt as dt,pl as pl,ca as ca, ac as ac, count(1) ac ct from tmp group by dt,pl,ca,ac,union all
select dt as dt,pl as pl,ca as ca, 'all' as ac, count(1) ac ct from tmp group by dt,pl,ca,union all

select dt as dt,'all' as pl,ca as ca, ac as ac, count(1) ac ct from tmp group by dt,ca,ac,union all
select dt as dt,'all' as pl,ca as ca, 'all' as ac, count(1) ac ct from tmp group by dt,ca
) as tmp1
//insert overwrite table `stats_event` partition(month=08,day=17)
//select *
insert overwrite table
select convert_date(dt),convert_platform(pl),convert_event(ca,ac),sum(ct),"2018-08-20"
group by pl,dt,ca,ac
;

sqoop语句：
import：
expor：

sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--driver com.mysql.jdbc.Driver --username root --password 123456 \
--table stats_event --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_event/* \
--input-fields-terminated-by '\\01' --update-mode allowinsert \
--update-key date_dimension_id,platform_dimension_id,event_dimension_id
;