show databases;

-- 1.练习完成Hive三种方式运行,并说明不同点

-- 2.练习使用hive视图的新增和删除,通过视图定制一个复杂查询
//拷贝一个新的数据表
create table if not exists t
as select * from jsonmovie;

select * from t;

// 创建一个视图
create view if not exists view1
as
    select movie,rate from t where rate > 3;

show create table view1; //查看视图的结构和相关属性
desc view1; //查看字段信息
select * from view1; //查看视图的具体信息
drop view view1; //删除视图

select * from view1 where rate = 5; //视图的查询

-- 3.练习用Vscode插件查看hive日志,并且高亮显示时间,级别等
-- 4.复习前期讲过zk完全分布式 hadoop完全分布式搭建
-- 5.简述Hive的不少于5种的优化方式.
