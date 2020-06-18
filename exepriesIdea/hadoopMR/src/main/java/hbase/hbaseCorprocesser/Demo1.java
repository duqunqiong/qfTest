package hbase.hbaseCorprocesser;

import hbase.table.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author: DuQunQinong
 * @date: 2020/3/31 21:43
 */

// 创建一个协处理器，创建好之后，再添加数据

public class Demo1 extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        // 1. 获取put对象 （follows表）的 行键和列值
        // 1.1 获取行键
        byte[] rowkey = put.getRow();
        // 1.2 获取到列的cell （不同版本）
        List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        Cell cell = cells.get(0);
        byte[] value = CellUtil.cloneValue(cell);

        // 2. 获取到，向fans表中插入put对象
        Put newPut = new Put(value);
        newPut.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("start"),rowkey);

        // 3. 提交表
        try {
            Table fans = HBaseUtil.getTable("fans");
            fans.put(newPut);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // 4. 释放资源
        HBaseUtil.closeTable(fans);

    }
}
