show databases ;
create database hm14;
use hm14;
show tables ;

//创建数据表

create table if not exists Student
(
    Sid       int,
    Sname     string,
    Sbirthday string,
    Ssex      string
)
row format delimited fields terminated by ' ';

create table if not exists Teacher
(
    Tid   int,
    Tname string
)
row format delimited fields terminated by ' ';

create table if not exists Course
(
    Cid   int,
    Cname string,
    Tid   int
)
row format delimited fields terminated by ' ';

create table if not exists SC
(
    Sid int,
    Cid int,
    score int
)
row format delimited fields terminated by ' ';

drop table Student;
drop table Teacher;
drop table Course;
drop table SC;

//加载数据
select * from Student;
select * from Teacher;
select * from Course;
select * from SC;
load data local inpath '/hivedata/student.txt' into table Student;
load data local inpath '/hivedata/teacher.txt' into table Teacher;
load data local inpath '/hivedata/course.txt' into table Course;
load data local inpath '/hivedata/sc.txt' into table SC;

-- 1、查询男生、女生人数：
select Ssex,count(*) from Student group by Ssex;

-- 2、查询出选修一门课程的全部学生的学号和姓名：
select Student.Sid, Student.Sname
from SC
         left join Student on SC.Sid = Student.Sid
where Cid = 1;


-- 3、查询1981年出生的学生名单
select * from Student where Sbirthday = '1981';

-- 4、查询平均成绩大于80的所有学生的学号、姓名和平均成绩：
select St.Sid,St.Sname,scc.avg
from Student St
         inner join (select Sid, avg(score) avg from SC group by Sid having avg > 80) scc on scc.Sid = St.Sid;

-- 5、查询每门课程的平均成绩，结果按平均成绩升序排序，平均成绩相同时，按课程号降序排列：
select c.Cid, c.Cname, round(scc.avg,1)
from Course c
         inner join (select Cid, avg(score) avg from SC group by Cid order by avg desc ) scc on c.Cid = scc.Cid
order by c.Cid asc ;

-- 6、查询课程名称为“数学”，且分数低于60的学生名字和分数：
select s.Sname,ct.score
from Student s,
     (select SC.Sid, c.Cname, SC.score
      from SC
               inner join (select Cid, Cname from Course where Cname = '数学') c on c.Cid = SC.Cid
      where SC.score < 60) ct
where s.Sid = ct.Sid;

-- 7、查询所有学生的选课情况：
select st.Sname,ct.Cname
from Student st,
     (select SC.Sid, Course.Cname
      from SC
               left join Course on SC.Cid = Course.Cid) ct
where st.Sid = ct.Sid ;

-- 8、查询任何一门课程成绩在70分以上的姓名、课程名称和分数：
select Sname,Cname,min
from Student st,
     (select SC.Sid, Course.Cname, MIN(SC.score) min
      from SC
               left join Course on Course.Cid = SC.Cid
      group by SC.Sid, Cname
      having min > 70) cc
where cc.Sid = st.Sid;

-- 9、查询01课程比02课程成绩高的所有学生的学号
select s1.Sid
from (select Sid, score from SC where Cid = 01) s1,
     (select Sid, score from SC where Cid = 02) s2
where s1.Sid = s2.Sid
  and s1.score > s2.score;

-- 10、查询平均成绩大于60分的同学的学号和平均成绩
select Sid, avg(score) avg
from SC
group by Sid
having avg > 60;

-- 11、查询所有同学的学号、姓名、选课数、总成绩
select st.Sid, Sname, count, sum
from Student st,
     (select Sid, count(Cid) count, sum(score) sum from SC group by Sid) scc
where st.Sid = scc.Sid;

-- 12、查询所有课程成绩小于60的同学的学号、姓名：
select st.Sid,Sname
from Student st
         right join (select Sid, max(score) max
                     from SC
                     group by Sid
                     having max < 60) scc
on scc.Sid = st.Sid;

-- 13、查询没有学全所有课的同学的学号、姓名：
select st.Sid,Sname from Student st ,(select Sid, count(SC.Cid) count_sc
                         from SC
                         group by Sid
                         having count_sc < 3) scc
where st.Sid=scc.Sid;

-- 14、查询至少有一门课与学号为01同学所学相同的同学的学号和姓名：
select distinct st.Sid, st.Sname
from Student st
         inner join SC c on st.Sid = c.Sid
where st.Sid != 01
  and c.Cid in (select s.Cid from SC s where s.Sid = 01);


-- 15、查询至少学过学号为01同学所有一门课的其他同学学号和姓名；
select st.Sid , st.Sname
from Student st,
     (select s.Sid from SC s where s.Cid not in (select Cid from SC where Sid = 01)) scc
where st.Sid != scc.Sid;

