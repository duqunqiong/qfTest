show databases ;
use hw12;
show tables ;

-- 练习多表导入
//创建表1
create table if not exists t1(
    id int,
    name string
)row format delimited fields terminated by ' ';
//加载数据
load data local inpath '/hivedata/text1.txt' into table t1;
select * from t1;

//创建表2
create table if not exists t2(uname string);

//创建表3
create table if not exists t3(id    int, uname string);

//多表导入
from t1
insert into t2 select name
insert into t3 select id,name where id <4;

//查看结果
select * from t2;
select * from t3;

-- 练习导出文件到本地和HDFS
// 数据导出到本地文件
insert overwrite local directory '/hivedata/exopt'
select * from t2;

//数据导出到hdfs文件
insert overwrite directory '/data/expot'
select * from t3;

-- 练习使用JSon格式数据导入
//创建一个表用来上传数据
create table if not exists t_json(json string);

//上传数据
load data local inpath '/hivedata/rate.txt' into table t_json;
select * from t_json;
drop table t_json;

//加载函数
add jar /hivedata/jar/countword-1.0-SNAPSHOT.jar;
create temporary function movieUDF as 'hive.homework19.MovieRateUDF';
SHOW FUNCTIONS ;

//插入临时表, 并拷贝原来的函数数据到临时表中
create table if not exists t_json_temp as
select movieUDF(json) as jsonLine
from t_json;
//检验结果
select * from t_json_temp;

//导入新的表，完成分割
create table if not exists jsonMovie as
select split(jsonLine, '\t')[0] as movie,
       split(jsonLine, '\t')[1] as rate,
     split(jsonLine,'\t')[2] as t_timestamp ,
     split(jsonLine,'\t')[3] as uid
from t_json_temp;
//检验结果
select *from jsonMovie;

-- 练习使用CSV SerDe,JSONSerde数据导入
-- csv Serde
//创建临时表
create table if not exists t_csvSerder
(
    id   int,
    name string,
    age  int
) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' stored as textfile ;
//导入数据
load data local inpath '/hivedata/serder.csv' into table t_csvSerder;
select * from t_csvSerder limit 4;
drop table t_csvserder;

//创建表，导入数据
create table if not exists csvSerder(
    id int,
    name string,
    age int
) row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorChar" = ","
        )
stored as textfile;

//导入数据
load data local inpath '/hivedata/serder.csv' into table csvSerder;
select * from csvSerder limit 4;

-- JSONSerde
//导入jar包到class中
add jar /hivedata/jar/json-serde-1.3.8-jar-with-dependencies.jar;

//创建一个底层的json实现表
create table if not exists jsonT(
    pid int,
    content string
)row format serde "org.openx.data.jsonserde.JsonSerDe";

//加载数据
load data local inpath '/hivedata/serder.txt' overwrite into table jsonT;
select * from jsonT limit 3;

-- 练习使用新建Seq文件,导入自己创建的不小于10M数据,并在查找时间和存储两个方面和textfile文件做对比
//临时表数据的准备
create table if not exists t_src(
    id int,
    name string
)row format delimited fields terminated by ',';

load data local inpath '/hivedata/seq.txt' into table t_src;
select * from t_src limit 6;

//创建sequencefile
create table if not exists seq1(
    id int,
    name string
)
row format delimited fields terminated by ' '
stored as sequencefile ;

//数据上传必须是临时表导入
insert into table seq1 select id,name from t_src;
//查询全局数据
select * from seq1;

// 创建textfile
create table if not exists txt(
    id int,
    name string
)row format delimited fields terminated by ' '
stored as textfile ;

//上传数据
insert into table txt select id,name from t_src;
//全局查询
select * from txt;
