show databases ;
use default;
show tables ;
drop table default.scores;
drop table default.subject;
drop table default.student;
drop table default.teacher;
drop table hw1;

--第一题

//创建数据库school
create database school;

//给数据库设置comment
create database if not exists qf comment 'this is a datebase of my';

//查看数据库的结构
show databases like 'z*';

//查看创建数据库
show databases ;

//删除数据库
drop database qf;


--第二题
use school;

--创建学生表、老师表、成绩表、科目表

drop table student;
create table if not exists student
(
    s_id     int,
    s_name   string,
    birthday date,
    sex      string
) row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile;


drop table teacher;
create table if not exists teacher
(
    t_id      int,
    t_name   string

)
row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile ;


drop table subject;
create table if not exists subject
(
    sb_id   int,
    sb_name string,
    t_id int
)
row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile ;


drop table scores;
create table if not exists scores
(
    s_id  int,
    sb_id  int,
    score int
)
row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile ;

show tables ;
desc teacher;
desc student;
desc scores;
desc subject;
desc formatted teacher;
show create table teacher;

//练习使用本地文件导入Hive
select * from student;
load data local inpath '/hivedata/student.txt' into table student;

select * from teacher;
load data local inpath '/hivedata/teacher.txt' into table teacher;

select * from scores;
load data local inpath '/hivedata/socred.txt' into table scores;

select * from subject;
load data local inpath '/hivedata/subject.txt' into table subject;

//hdfs文件导入到Hive
use school;
show tables ;
drop table subject2;
create table if not exists subject2
(
    sb_id   int,
    sb_name string,
    t_id int
) row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile;

drop table scores2;
create table if not exists scores2
(
    s_id  int,
    sb_id  int,
    score int
) row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile;


drop table teacher2;
create table if not exists teacher2
(
    t_id   int,
    t_name string
)
row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile ;


drop table student2;
create table if not exists student2
(
    s_id     int,
    s_name   string,
    birthday date,
    sex      string
)
row format delimited fields terminated by ' ' lines terminated by '\n' stored as textfile ;

drop table subject2;
select * from subject2;
load data inpath '/hdfs/upload/hivedata/subject.txt' into table subject2;
select * from scores2;
load data inpath '/hdfs/upload/hivedata/socred.txt' into table scores2;
select * from student2;
load data inpath '/hdfs/upload/hivedata/student.txt' into table student2;
select * from teacher2;
load data inpath '/hdfs/upload/hivedata/teacher.txt' into table teacher2;


//克隆表带数据
use school;
show tables ;
create table if not exists studentall as
select *
from school.student;

create table if not exists teacherall as
select *
from school.teacher;

create table if not exists scoresall as
select *
from school.scores;

create table if not exists subjectall as
select *
from school.subject;

select * from studentall;

//克隆表不带数据等语句
use school;
show tables ;
create table if not exists studentnew like student;
create table if not exists teachernew like teacher;
create table if not exists scoresnew like scores;
create table if not exists subjectnew like subject;

select * from studentnew;


