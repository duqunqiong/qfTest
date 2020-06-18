show databases ;
create database hw12;
use hw12;
show tables ;

//一级分区
-- 1，创建一个静态分区表，练习用脚本操作,添加多个分区,删除多个分区,修改分区等操作
create table if not exists part1
(
    id   int,
    name string,
    address string
) partitioned by (dt string)
    row format delimited fields terminated by ' ';

load data local inpath '/hivedata/part1.txt' into table part1 partition (dt = "20110-03-09");
load data local inpath '/hivedata/part2.txt' into table part1 partition (dt = "20110-03-10");
select * from part1;
show partitions part1;

//新增分区
alter table part1 add partition (dt="12-00");
alter table part1
    add partition (dt = "12-01") partition (dt="12-02");

//增加分区并设置数据
alter table part1
    add partition (dt = "12-03") location '/user/hive/warehouse/hw.db/part1/dt=20110-03-09';

//修改分区的数据路径
alter table part1 partition (dt="20110-03-10")
    set location 'hdfs://Hadoop601:8020/user/hive/warehouse/hw12.db/part1/dt=20110-03-09';

//删除分区
alter table part1 drop partition (dt='12-00'),partition (dt='12-01'),partition (dt='12-02'),partition (dt='12-03');
alter table part1 drop partition (dt='12-00'),partition (dt='12%3A00'),partition (dt='12-04');

drop database hw cascade;

--创建一级分区,二级分区,三级分区,并且用对这三个分区进行加载数据
drop table part2;
drop table part3;
drop table part4;
//一级分区
create table if not exists part2
(
    id     int,
    name   string,
    address string
) partitioned by (dt string)
    row format delimited fields terminated by ' ';

//二级分区
create table if not exists part3
(
    id      int,
    name    string,
    address string
) partitioned by (year string,month string)
    row format delimited fields terminated by ' ';

//三级分区
create table if not exists part4
(
    id      int,
    name    string,
    address string
) partitioned by (year string, month string,day string)
row format delimited fields terminated by ' ';

//数据加载
select * from part2;
select * from part3;
select * from part4;

load data local inpath '/hivedata/user.txt'
    into table part2 partition (dt = "2019-3-10");

load data local inpath '/hivedata/user.txt'
    into table part3 partition (year='2019',month="03");

load data local inpath '/hivedata/user.txt'
    into table part4 partition (year='2019',month="03",day ="10");


--第三题：练习创建动态分区,并且用临时表进行动态分区表数据的插入
//设置动态属性
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

//创建动态分区
create table if not exists dy_part
(
    id   int,
    name string,
    age  int
)partitioned by (dt string)
    row format delimited fields terminated by ' ';

//创建临时表
drop table temp_part;
create table if not exists temp_part
(
    id   int,
    name string,
    age  int,
    dt string
) row format delimited fields terminated by ' ';

select * from temp_part;
select * from dy_part;

//数据加载到临时表
load data local inpath '/hivedata/user.txt' into table temp_part;

//数据加载到动态表
insert into dy_part partition (dt)
select id, name, age,dt
from temp_part;

--使用混合分区,并进行数据的加载
drop table temp_part2;
drop table dy_part2;
//设置动态属性
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

//创建一个混合分区
create table dy_part2
(
    id   int,
    name string,
    age  int
) partitioned by (year string,month string,day string )
    row format delimited fields terminated by ' ';

//创建一个临时表
create table if not exists temp_part2
(
    id   int,
    name string,
    age  int,
    year string,
    month string,
    day string
)row format delimited fields terminated by ' ';

//数据加载
load data local inpath '/hivedata/user.txt' overwrite into table temp_part2;
select * from temp_part2;

insert into dy_part2 partition (year = '2019', month, day)
select id, name, age,  month, day
from temp_part2;
select * from dy_part2;
