show databases ;
show tables ;
use hm15;

create table if not exists maxValue(
    value int
);

load data local inpath '/hivedata/maxValue' into table maxValue;

select * from maxValue;

drop table maxValue;

-- 添加jar包到hive的class路径
add jar /hivedata/countword-1.0-SNAPSHOT.jar;

-- 创建一个临时函数名，与上面的hive在同一个session里面
create temporary function MaxValue as 'hive.homework18.MaxValue';
create temporary function Key2Value as 'hive.homework18.Key2Value';
create temporary function LogParser as 'hive.homework18.LogParser';
create temporary function Agenum as 'hive.homework17.UDF_Age';
create temporary function word as 'hive.homework17.UDTF_word';

--查看功能
show functions ;

-- 使用该功能
select MaxValue(value) from maxValue;
SELECT Key2Value("sex=1&hight=180&weight=130&sal=28000","weight");
select LogParser("220.181.108.151 - - [31/Jan/2012:00:02:32 +0800] \"GET /home.php?mod=space&uid=158&do=album&view=me&from=space HTTP/1.1\" 200 8784 \"-\" \"Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)\"");
select Agenum("1997-10-10");
select word("a,b,c,d,e");

-- 删掉该函数
drop temporary function if exists MaxValue;
drop temporary function if exists Key2Value;
drop temporary function if exists LogParser;
drop temporary function if exists Agenum;
drop temporary function if exists word;
