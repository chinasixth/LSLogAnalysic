用户浏览深度分析：
一个用户访问了几个url，访问的url个数就是浏览器深度，然后将深度相同的uid进行count，就得到深度为1的uid个数……

数据抽取(只抽取该模块中需要的字段即可)；
load data inpath '' into table logs partition(month=08,day18)

考虑是否需要udf函数


创建表：
需要创建一个结果表
CREATE TABLE IF NOT EXISTS `stats_view_depth` (
  `platform_dimension_id` int,
  `data_dimension_id` int,
  `kpi_dimension_id` int,
  `pv1` int,
  `pv2` int,
  `pv3` int,
  `pv4` int,
  `pv5_10` int,
  `pv10_30` int,
  `pv30_60` int,
  `pv60pluss` int,
  `created` date,
);


创建临时表：
CREATE TABLE IF NOT EXISTS `stats_view_depth_tmp` (
  dt string,
  pl string,
  col string,
  ct int
);

sql语句：
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

## 此时pv1……作为col的值，存储在col列中，要想根据col列值进行扩维，需要将每一个列值使用一条select语句查询出来，其中其它的列值按照一个默认值查询（即不能将其它的类值查询出来），然后进行union all
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

sqoop语句
sqoop export --connect jdbc:mysql://hadoop05:3306/result \
 --username root --password 123456 \
 --table stats_view_depth --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_view_depth/* \
 --input-fields-terminated-by '\\01' --update-mode allowinsert \
 --update-key   `platform_dimension_id`,`data_dimension_id`, `kpi_dimension_id`
 ;


用户角度下的浏览深度


