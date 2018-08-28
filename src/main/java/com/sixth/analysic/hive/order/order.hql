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
from_unixtime(case(l.s_time/1000 as bigint), 'yyyy-MM-dd') as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt
count(distinct o_id) as ct
from logs l
where l.month = 8
and l.day = 20
and l.o_id is not null
and l.o_id <> 'null'
and l.en = 'e_crt'
group by from_unixtime(case(l.s_time/1000 as bigint), 'yyyy-MM-dd'),pl,cut,pt
) as tmp
insert overwrite table stats_order_tmp
select convert_date(dt),convert_platform(pl),convert_currencytype(cut),convert_paymenttype(pt),sum(ct),dt
;


sqoop export --connect jdbc:mysql://hadoop05:3306/result \
 --username root --password 123456 \
 --table stats_order --export-dir hdfs://hadoop05:9000/home/hadoop/data/hivedata/hive/stats_order_tmp/* \
 --input-fields-terminated-by '\\01' --update-mode allowinsert \
 --update-key `data_dimension_id`,`platform_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`
 --columns '`data_dimension_id`,`platform_dimension_id`,`currency_type_dimension_id`,`payment_type_dimension_id`,`orders`,`created`'
 ;











