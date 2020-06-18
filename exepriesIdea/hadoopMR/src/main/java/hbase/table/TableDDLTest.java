package hbase.table;

import hbase.SuperSpaceTest;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * 定义一个类， 对hbase里面的表进行ddl操作
 */

/**
 * @author: DuQunQinong
 * @date: 2020/3/25 21:15
 */
public class TableDDLTest extends SuperSpaceTest {
    //定义一个通用的日志记录器，如果是频繁创建，一定要用static
    private Logger logger = LoggerFactory.getLogger(getClass());

    // 创建一个表
    @Test
    public void testCreateTable() {
        // 按照namespace 的规则， 创建表之前，先要定义一个命名空间
        //  相同的表名在一个数据库中只有一个， 如果通过new， 则会产生多个对象
        // *可以用工厂模式
        TableName usertable = TableName.valueOf("usertable");
        HTableDescriptor hTableDescriptor = new HTableDescriptor(usertable);

        //创建列族  userInfo , extraInfo
        HColumnDescriptor userInfo = new HColumnDescriptor("userInfo");
        HColumnDescriptor extraInfo = new HColumnDescriptor("extraInfo");

        //可以给列族设置属性
        userInfo.setVersions(1, 3);  // 最小版本号1， 最大版本号3
        userInfo.setTimeToLive(24 * 60 * 60);  // 存活时间为 一天

        //把创建号的列族加入到列表中
        hTableDescriptor.addFamily(userInfo);
        hTableDescriptor.addFamily(extraInfo);

        //通过hbase 管理对象hbaseAdmin 创建表
        try {
            hBaseAdmin.createTable(hTableDescriptor);
        } catch (IOException e) {
            logger.error("表创建错误：" + e.getMessage() + "\n", e);
        }
    }


    // 修改表的结构 ---- 后面添加的列族 会覆盖前面的列族
    @Test
    public void testModifyTable() throws IOException {
        //创建一个表的表描述器
        HTableDescriptor usertable = new HTableDescriptor(TableName.valueOf("usertable"));

        // 创建一个列簇
        HColumnDescriptor newColumn = new HColumnDescriptor("newColumn");

        //修改表的结构
        // * 当前表描述器是新建的，这样子修改表的列族，后面新加列簇覆盖之前的列簇
        hBaseAdmin.modifyTable(TableName.valueOf("usertable"), usertable);

        logger.info("修改Cloumn 成功");
    }


    // 修改表的结构 ---- 后面添加的列族 不会覆盖前面的列族
    @Test
    public void testModifyTableNotOverwrite() throws IOException {
        // 拿到原来的描述器
        HTableDescriptor usertable = hBaseAdmin.getTableDescriptor(TableName.valueOf("usertable"));

        //创建一个列簇描述器
        HColumnDescriptor user2Info = new HColumnDescriptor("user2Info");
        HColumnDescriptor user1Info = new HColumnDescriptor("user1Info");

        //把列族加入到表的描述器，后面的列簇不会覆盖前面的列簇
        usertable.addFamily(user2Info);
        usertable.addFamily(user1Info);

        //提交修改
        hBaseAdmin.modifyTable(TableName.valueOf("usertable"), usertable);
    }


    // 直接删除列族
    @Test
    public void testDeleteFamily() throws IOException {
        hBaseAdmin.deleteColumn(TableName.valueOf("usertable"), "user2Info".getBytes());
    }

    // 修改表的结构 ----- 删除列族
    @Test
    public void testDeleteFamilyFormTable() throws IOException {
        //拿到表的描述器
        HTableDescriptor usertable = hBaseAdmin.getTableDescriptor(TableName.valueOf("usertable"));

        //通过描述器把表中 列簇删除
        HColumnDescriptor hColumnDescriptor = usertable.removeFamily("user1Info".getBytes());

        // 提交表的修改
        hBaseAdmin.modifyTable(TableName.valueOf("usertable"), usertable);

    }

    // 表的查询 ------- 查询表中所有列族
    @Test
    public void testListFamily() throws IOException {
        // 拿到表的描述器
        HTableDescriptor usertable = hBaseAdmin.getTableDescriptor(TableName.valueOf("usertable"));

        // 通过表的描述其器 拿到所有的列簇
        HColumnDescriptor[] columnFamilies = usertable.getColumnFamilies();

        // 循环列出列簇
        for (HColumnDescriptor columnFamily : columnFamilies) {
            // logger 来代替system.out的输出
            logger.info("columnFamily = " + columnFamily);
            logger.info("VERSIONS = " + columnFamily.getMaxVersions());
        }
    }


    // 表的删除
    @Test
    public void testDeleteTable() throws IOException {
        // 删除表的名字
        String deleteTableName = "usertable";

        //表是否存在
        boolean tableExists = hBaseAdmin.tableExists(deleteTableName);

        // 判断是否存在，不存在抛异常
        if (!tableExists) {
            logger.warn("你要删除的表不存在： " + deleteTableName);
            return;
        }

        //查看表是否被禁用, 若未禁用，进行禁用操作
        if (!hBaseAdmin.isTableDisabled(TableName.valueOf(deleteTableName))) {
            logger.warn("表没有被禁用，现在disable...");
            hBaseAdmin.disableTable(TableName.valueOf(deleteTableName));
        }

        //删除表
        try {
            hBaseAdmin.deleteTable(TableName.valueOf(deleteTableName));
            logger.info("表删除成功");
        } catch (IOException e) {
            logger.error("表删除错误" + e);
        }
    }

}