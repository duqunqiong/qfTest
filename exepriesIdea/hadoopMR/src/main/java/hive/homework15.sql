show databases ;
create database hm15;
use hm15;
show tables ;

//创建一个表
create table if not exists arr1
(
    name  string,
    score array<string>
) row format delimited fields terminated by ' t'
    collection items terminated by ',';

//上传数据
load data local inpath '/hivedata/arr1.csv' into table arr1;

//查询数据
select * from arr1;
select name,score[1] from arr1;
drop table arr1;

--列转行
select explode(score) score from arr1; --一列多行
select name,cj from arr1 lateral view explode(score) score as cj; --多列多行

--行转列
//创建临时表，上传arr1的数据
create table if not exists arr3_temp
as
select name, cj
from arr1 lateral view explode(score) score as cj;

//创建导入的数据表
create table if not exists arr3
(
    name  string,
    score array<string>
) row format delimited fields terminated by ' '
    collection items terminated by ',';

//将临时表的数据转到新的表中，行变成数组array
insert into arr3
select name, collect_set(cj)
from arr3_temp
group by name;

//查看数据
select * from arr3;

-- map类型
//创建一个表array
create table if not exists map1
(
    name  string,
    score map<string,int>
) row format delimited fields terminated by ' '
    collection items terminated by ','
    map keys terminated by ':';

//加载数据
load data local inpath '/hivedata/map.csv' into table map1;
select * from map1;//检验数据
drop table map1;

//数据查询
select name, score['chinese'], score['math']
from map1
where score['math'] > 35;

--map 列转行
select explode(score) as (class,mscore) from map1;

select name, class, mscore
from map1 lateral view explode(score) score as class, mscore;


--map 行转列
//创建临时表
create table map2_temp(
name string,
score1 int,
score2 int,
score3 int
)row format delimited fields terminated by ',';

//加载数据
load data local inpath '/hivedata/map2.csv' into table map2_temp;
select * from map2_temp;

//创建map导入的表
create table if not exists map2
(
    name  string,
    score map<string,int>
) row format delimited fields terminated by ' '
    collection items terminated by ','
    map keys terminated by ':';

//加载导入数据
insert into map2
select name, map('chinese', score1, 'math', score2, 'english', score3)
from map2_temp;

select * from map2;

-- 复杂数据类型的导入
//创建导入表
create table if not exists complex (
    id int,
    name string,
    belong array<string>,
    tax map<string,double>,
    add struct<province:string,city:string,road:string>
)row format delimited fields terminated by ' '
collection items terminated by ','
map keys terminated by ':'
stored as textfile ;

load data local inpath '/hivedata/complicate.txt' into table complex;
select * from complex;

--日期函数的使用
//获取当前的时间
select current_date();

//获取当前时间戳
select unix_timestamp();

//时间戳转时间
select from_unixtime(1583865770);

//日期转时间戳
select unix_timestamp('2020-03-11 02:42:50');

//计算时间差
select datediff('2020-03-11', '2020-02-11');

//查询当前是该月第几天
select dayofmonth(current_date);

-- 练习使用常见的字符串函数和类型转换函数
//字符串转日期
select cast("2020-01-01" as date);
select cast(2020/01/01 as string);

//字符串转int
select cast("2010" as int);
select cast(123 as string);

//string类型转换boolean类型
select cast("true" as boolean);
select cast(true as string);

//大写转小写
select lower("ABCDE");
select upper("abcde");

//字符串的拼接
select concat("12_3","ab_c3","q3_q");
select concat_ws('_',"12","3ab","c3q3q");

//切割
select split("123-234-345-456","-");
select substr("aaabbbcccdd",1,4);
select substr("aaabbbcccdd",4);
