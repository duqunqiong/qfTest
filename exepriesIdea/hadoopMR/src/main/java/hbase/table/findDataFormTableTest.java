package hbase.table;

import hbase.SuperSpaceTest;
import hbase.namespace.HbaseTets;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author: DuQunQinong
 * @date: 2020/3/26 17:19
 */

// get查询

public class findDataFormTableTest extends SuperSpaceTest {

    // 定义一个logger ， 一个table
    private  Logger logger = LoggerFactory.getLogger(getClass());
    private static Table table;

    {
        try {
            table = getTable("usertable");
        } catch (IOException e) {
           logger.error("出现错误 ：" + e);
        }
    }

    // 第一种查询
    @Test
    public void getData1() throws IOException {
        // 1. 获取get对象
        Get get = new Get(Bytes.toBytes("001"));

        // 2. 通过table获取结果的对象
        Result result = table.get(get);

        // 3. 获取表格扫描器
        CellScanner cellScanner = result.cellScanner();
        logger.info("roeKey = " + result.getRow());

        // 4. 遍历所有的数据
        while (cellScanner.advance()){

            // 4.1 获取当前的表格
            Cell current = cellScanner.current();

            // 4.2 获取所有的列簇 + 输出列簇的名字、长度、以及位置
            byte[] familyArray = current.getFamilyArray();
            logger.info(new String(familyArray, current.getFamilyOffset(), current.getFamilyLength()));

            // 4.3 获取所有的列 + 输出所有列的名字 + offset + 长度
            byte[] qualifierArray = current.getQualifierArray();
            logger.info(new String(qualifierArray, current.getQualifierOffset(), current.getQualifierLength()));

            // 4.4 获取表中的所有值
            byte[] valueArray = current.getValueArray();
            logger.info(new String(valueArray, current.getValueOffset(), current.getValueLength()));
        }

    }

    // 第三种查询
    @Test
    public void getData3() throws IOException {
        // 1. 获取get对象
        Get get = new Get(Bytes.toBytes("002"));

        // 2. 通过table获取结果的对象
        Result result = table.get(get);

        // 3. 获取表格的扫描器
        CellScanner cellScanner = result.cellScanner();
        logger.info("rowkey = " + result.getRow());

        // 4. 遍历
        while (cellScanner.advance()){

            // 4.1 获取当前表格
            Cell current = cellScanner.current();

            // 4.2 h获取所有的列簇
            logger.info(new String(CellUtil.cloneFamily(current), "utf-8"));
            logger.info(new String(CellUtil.cloneQualifier(current), "utf-8"));
            logger.info(new String(CellUtil.cloneValue(current), "utf-8"));
        }
    }

    // 批量 get 查询
    @Test
    public void batchGetData() throws IOException {

        // 1. 创建一个集合 ， 来存储get 对象
        ArrayList<Get> gets = new ArrayList<>();

        // 2. 创建多个get对象
        Get get01 = new Get(Bytes.toBytes("001"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sal"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sex"));

        Get get02 = new Get(Bytes.toBytes("002"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("name"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sal"));

        Get get03 = new Get(Bytes.toBytes("003"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("name"));
        get01.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sex"));

        Get get04 = new Get(Bytes.toBytes("004"));

        // 3. 将创建的 get对象 添加到集合当中去
        gets.add(get01);
        gets.add(get02);
        gets.add(get03);
        gets.add(get04);

        // 4. 将集合对象 添加到表中
//        Result[] results = table.get(gets);

        Result[] results = null;
        try {
           results = table.get(gets);
        } catch (IOException e) {
            logger.error("get对象添加到表中失败：" + e);
        }

        // 5. 遍历
        for (Result result:results) {
            showResult(result);
        }

    }


    // * 修改HBaseUtils
    public  void showResult(Result result){

        // 得到表的扫描器
        CellScanner cellScanner = result.cellScanner();
        logger.info("rowKey = " + result.getRow());

        //遍历
        while (true){
            try {
                if (!cellScanner.advance()){
                    logger.error("判断是否有下一个元素失败！" );
                    break;
                }

                Cell cell = cellScanner.current();
                logger.info("\t" + new String(CellUtil.cloneFamily(cell),"UTF-8"));
                logger.info("\t" + new String(CellUtil.cloneQualifier(cell),"UTF-8"));
                logger.info("\t" + new String(CellUtil.cloneValue(cell),"UTF-8"));

            } catch (IOException e) {
                logger.error("克隆数据失败" + e);
            }
        }

    }


    // scan 查询
    @Test
    public void scanTable() {
        // 1. 创建扫描器
        Scan scan = new Scan();

        // 2. 添加扫描的行数，包头不包尾
        scan.setStartRow(Bytes.toBytes("001"));
        scan.setStopRow(Bytes.toBytes("006"+"\001" ));

        // 3. 添加扫描的列
        scan.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("uname"));
        scan.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sal"));

        // 4. 获取扫描器
        try {
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();
            while (iterator.hasNext()){
                Result result = iterator.next();
                showResult(result);
            }
        } catch (IOException e) {
            logger.error("获取扫描器失败：" + e);
        }
    }


    // 删除数据
    @Test
    public void deleteData (){
        // 1. 创建适合批量删除的 delete对象 集合
        ArrayList<Delete> deletes = new ArrayList<>();

        // 2. 创建删除数据对象
        Delete delete1 = new Delete(Bytes.toBytes("001"));
        delete1.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sal"));
        Delete delete2 = new Delete(Bytes.toBytes("002"));
        delete2.addColumn(Bytes.toBytes("userInfo"),Bytes.toBytes("sal"));

        // 3. 添加到 list 集合中去
        deletes.add(delete1);
        deletes.add(delete2);

        // 4. 提交
        try {
            table.delete(deletes);
        } catch (IOException e) {
            logger.error("删除失败：" + e);
        }

        logger.info("删除成功");
    }

}
