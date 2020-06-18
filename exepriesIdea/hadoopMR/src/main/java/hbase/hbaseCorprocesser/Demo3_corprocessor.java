package hbase.hbaseCorprocesser;

import hbase.table.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
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
 * @date: 2020/4/1 1:22
 */
public class Demo3_corprocessor extends BaseRegionObserver {

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {

    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put, WALEdit edit, Durability durability) throws IOException {
        //1、获取粉丝的put对象,并解析成key-value
        byte[] rowkey = put.getRow();  //ergouzi
        List<Cell> cells = put.get(Bytes.toBytes("cf"), Bytes.toBytes("star"));
        //获取cells中的值
        Cell cell = cells.get(0);
        byte[] bytes = CellUtil.cloneValue(cell); //wangbaoqiang

        //2、往fans表中添加对应的数据
        Put put1 = new Put(bytes); //wangbaoqiang
        put1.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("fensi"),rowkey); //ergouzi

        //3、将put1然后创建表并提交
        try {
            Table fans = HBaseUtil.getTable("fans");
            fans.put(put1); //将粉丝put对象放到fans
            //关闭
            HBaseUtil.closeTable(fans);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
