show databases;
use hm15;
show tables ;

//创建一个新的order表
create table if not exists order_t
(
    name  string,
    data  string,
    value int
) row format delimited fields terminated by ',';

//上传数据
load data local inpath '/hivedata/order.txt' into table order_t;
select * from order_t;
drop table order_t;

-- over 与 group 之间的的区别
//over
select *,count(value) over (partition by name ) as num from order_t;
//group
select name,count(value) num from order_t group by name;

--多组数据开窗展示
select *,
       count(value) over (partition by name order by data)                                                 as num,
       sum(value) over (partition by name )                                                                as sum1,
       sum(value) over (partition by name order by data)                                                   as sum2,
       sum(value) over (partition by name order by data rows between UNBOUNDED preceding and current row ) as sum3
from order_t ;

--统计用户前三条和后三条的数据总和
select *,
       sum(value) over (partition by name order by data rows between UNBOUNDED preceding and 2 following) as start,
       sum(value) over (partition by name order by data rows between 2 preceding and UNBOUNDED FOLLOWING) as last
from order_t;

-- 5月份每个人最早购买的时间和最迟购买的时间
select *,  --方法一
       min(data) over (partition by name ) as first,
       max(data) over (partition by name ) as last
from order_t
where substr(data, 1, 7) = "2018-05" ;

select *, --方法二
       first_value(data) over (partition by name order by data) first,
       last_value(data) over (partition by name order by data) last
from order_t
where substr(data,6,2) = "05";

-- 按照名字切分4片
select *,
       ntile(4) over (partition by name) as pile
from order_t;

select *,
       ntile(2) over (partition by name order by data) as pilenum
from order_t;

-- 每个人一半(50%)的购买细节.
select *
from (select *,
             ntile(2) over (partition by name order by data) as pilenum
      from order_t) ordernew
where ordernew.pilenum = 1;
