show databases ;
use hm15;
show tables ;
create table if not exists grade(
    sid int,
    sname string,
    brirthday string,
    sex string,
    sub string,
    score int
) row format delimited fields terminated by '\t';
drop table grade;

load data local inpath '/hivedata/grade.txt' into table grade;

-- 按照原来方法排序
select * from grade order by score,sub;
select sname,score,sid from grade order by score,sub desc ;


select *,
-- 没有并列，相同名次依次排序
       row_number() over (distribute by sub sort by score desc ) as rn,
-- 有并列，相同名次空位
        rank() over (distribute by sub sort by score desc ) as rn1,
-- 有并列，相同名词没有空位
        dense_rank() over (distribute by sub sort by score desc ) as  rn2
from grade;

-- 找出每个科目的前三名
select sname,sub,score,gra.num
from (select *,
             rank() over (distribute by sub sort by score desc ) as num
        from grade) gra
where gra.num < 4;


//第二题
-- 将jar包添加到hive的class中
add jar /hivedata/countword-1.0-SNAPSHOT.jar;

-- 创建一个临时函数名，与上面的hive在同一个session里面
create temporary function countAge as 'hive.homework17.UDF_Age';

--查看功能
show functions ;

-- 使用该功能
select countage('1990-10-10');

-- 删掉该函数
drop temporary function if exists countAge;

//第三题
-- 将jar包添加到hive的class中
add jar /hivedata/countword-1.0-SNAPSHOT.jar;

-- 创建一个临时函数名，与上面的hive在同一个session里面
create temporary function wordDo as 'hive.homework17.UDTF_word';

--查看功能
show functions ;

-- 使用该功能
select wordDo('3,a,b,c,d');

-- 删掉该函数
drop temporary function if exists wordDo;
