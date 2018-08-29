订单数量和订单总金额：
订单数量：将所有的订单id去重，
订单金额：将所有订单的金额sum
需要使用的字段：pl  s_time  cut  pt  oid  cua

数据抽取：

考虑是否需要udf函数：

创建表：
首先创建一张订单数的结果表：
CREATE TABLE IF NOT EXISTS `stats_order` (
  `platform_dimension_id` int,
  `date_dimension_id` int,
  `currency_type_dimension_id` int,
  `payment_type_dimension_id` int,
  `orders` int,
  `created` date
)

创建临时表：
CREATE TABLE IF NOT EXISTS `stats_order_tmp`(
  `date_dimension_id` int,
  `platform_dimension_id` int,
  `currency_type_dimension_id` int,
  `payment_type_dimension_id` int,
  `ct` int,
  `created` string
)
;

写指标：
总订单数量
from(
select
from_unixtime(cast(l.s_time / 1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct oid) as ct
from logs l
where l.month = 8
and l.day = 20
and l.oid is not null
and l.oid <> 'null'
and l.en = 'e_crt'
group by from_unixtime(cast(l.s_time/1000 as bigint), 'yyyy-MM-dd'),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select convert_date(dt),convert_platform(pl),convert_currencytype(cut),convert_paymenttype(pt),sum(ct),dt
group by pl,cut,pt,dt
;


sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_order --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,orders,created'
;


成功支付订单数量:
from(
select
from_unixtime(cast(l.s_time / 1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct oid) as ct
from logs l
where l.month = 8
and l.day = 20
and l.oid is not null
and l.oid <> 'null'
and l.en = 'e_cs'
group by from_unixtime(cast(l.s_time/1000 as bigint), 'yyyy-MM-dd'),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select convert_date(dt),convert_platform(pl),convert_currencytype(cut),convert_paymenttype(pt),sum(ct),dt
group by pl,cut,pt,dt
;


sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_order --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,success_orders,created'
;



退款订单数量:
from(
select
from_unixtime(cast(l.s_time / 1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct oid) as ct
from logs l
where l.month = 8
and l.day = 20
and l.oid is not null
and l.oid <> 'null'
and l.en = 'e_cr'
group by from_unixtime(cast(l.s_time/1000 as bigint), 'yyyy-MM-dd'),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select convert_date(dt),convert_platform(pl),convert_currencytype(cut),convert_paymenttype(pt),sum(ct),dt
group by pl,cut,pt,dt
;


sqoop export --connect jdbc:mysql://hadoop05:3306/result \
--username root --password 123456 \
--table stats_order --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_order_tmp/* \
--input-fields-terminated-by "\\01" --update-mode allowinsert \
--update-key platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id \
--columns 'platform_dimension_id,date_dimension_id,currency_type_dimension_id,payment_type_dimension_id,refund_orders,created'
;



将三条select语句合成一条：
要想使用一条语句同时统计三个指标，肯定是根据en类型统计相应的值，那么en的类型统计的结果对应一个列，每一个存放结果的列是根据en的值来确定的，
en的值不同，那么对应的列也是不同的，这样的话，一行数据中en的类型只有一个，所以只有这个en对应的列才有值，其他的列给定一个默认值就行了
2018-08-20  website  RMB  alipay  e_crt  2  0  0
2018-08-20  website  RMB  alipay  e_cs   0  2  0
2018-08-20  website  RMB  alipay  e_cr   0  0  1
2018-08-20  website  $    alipay  e_crt  3  0  0
2018-08-20  website  $    alipay  e_cs   0  3  0
2018-08-20  website  $    alipay  e_cr   0  0  1

将上面的数据合成一行，这样方便往数据库中存储
    dt         pl    cut    pt   crt cs cr
2018-08-20  website  RMB  alipay  2  2  1
2018-08-20  website  $    alipay  3  3  1

with tmp as(
select
from_unixtime(cast(l.s_time / 1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
l.en as en
if((case when en='e_crt' count(e_crt) then count(distinct oid) end) is null, 0, (case when en='e_crt' count(e_crt) then count(distinct oid) end)) as orders,
if((case when en='e_cs' count(e_cs) then count(distinct oid end) is null, 0, (case when en='e_cs' count(e_crt) then count(distinct oid) end)) as success_orders,
if((case when en='e_cr' count(e_cr) then count(distinct oid end) is null, 0, (case when en='e_crt' count(e_cr) then count(distinct oid) end)) as refund_orders
from logs l
where l.month = 8
and l.day = 20
and l.oid is not null
and l.oid <> 'null'
group by from_unixtime(cast(l.s_time/1000 as bigint), 'yyyy-MM-dd'),pl,cut,pt,en
)
from(
select dt as dt, pl as pl,cut as cut,pt as pt,orders as orders,success_orders as success_orders, refund_orders as refund_orders from tmp where en = 'e_crt'
union all
select dt as dt, pl as pl,cut as cut,pt as pt,orders as orders,success_orders as success_orders, refund_orders as refund_orders from tmp where en = 'e_cs'
union all
select dt as dt, pl as pl,cut as cut,pt as pt,orders as orders,success_orders as success_orders, refund_orders as refund_orders from tmp where en = 'e_cr'
)
insert overwrite table stats_order_tmp1
select convert_date(dt),convert_platform(pl),convert_currencytype(cut),convert_platform(pt),sum(orders),sum(success_orders),sum(refund_orders),dt
group by dt,pl,cut,pt




