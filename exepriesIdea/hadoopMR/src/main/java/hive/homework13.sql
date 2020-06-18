show databases ;
drop database hw13 cascade ;
create database hw13;
use hw13;

--设置强制分桶的属性
set hive.enforce.bucketing=true;


--第一题： 创建一个分桶表,并且练习分桶表的数据插入
show tables ;
//创建一个分桶表
drop table buc1;
drop table temp_buc1;
create table if not exists buc1
(
    id      int,
    name    string,
    age string
)
    clustered by (id) into 8 buckets
    row format delimited fields terminated by ' ';

//创建临时表
create table if not exists temp_buc1
(
    id      int,
    name   string,
    age string
)row format delimited fields terminated by ' ';


//加载数据
select * from buc1;
select * from temp_buc1;
load data local inpath '/hivedata/buc.txt' into table temp_buc1;

//数据查询
insert overwrite table buc1
select id, name, age
from temp_buc1 cluster by (id);


--第二题： 求分三种情况抽样,可以取一桶,取4桶,取8桶的情况下

//取一桶
select * from buc1 tablesample ( bucket 1 out of 8 on id);

//取四桶
select * from buc1 tablesample ( bucket 1 out of 2 on id);

//取八桶
select * from buc1 tablesample ( bucket 1 out of 1 on id);
