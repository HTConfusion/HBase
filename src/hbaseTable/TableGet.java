package hbaseTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 单行查询
 */
public class TableGet {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("tablename");
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes("row-4"));
        get.addFamily(Bytes.toBytes("fam1"));
        Result result = table.get(get);
        //------------------------------------------------------
//        for (Cell c : result.rawCells()) {//result.listCells();
//            //row-4/fam1:col-1/1517478166779/Put/vlen=7/seqid=0
//            System.out.println(c);
//            //可直接输出时间戳
//            System.out.println(c.getTimestamp());
//            //列长度--列起始位置
//            System.out.println(c.getQualifierLength()+"--"+c.getQualifierOffset());
//        }
        /**
         * 取值
         */
        //CellUtil--------------------------------------------------
//        for(Cell c:result.rawCells()){
//            //获取行键，列族，列，值
//            System.out.println(new String(CellUtil.cloneRow(c)));
//            System.out.println(new String(CellUtil.cloneFamily(c)));
//            System.out.println(new String(CellUtil.cloneQualifier(c)));
//            System.out.println(new String(CellUtil.cloneValue(c)));
//        }
        //Cell------------------------------------------------------
//        for (Cell c : result.rawCells()) {
//            //过期--但可用
//            System.out.println(Bytes.toString(c.getRow()));
//            System.out.println(Bytes.toString(c.getFamily()));
//            System.out.println(Bytes.toString(c.getQualifier()));
//            System.out.println(Bytes.toString(c.getValue()));
//            System.out.println(c.getTimestamp());
//        }
        //KeyValue--------------------------------------------------
//        for (KeyValue k : result.raw()) {//Cell c : result.raw()
//            //过期--但可用
//            System.out.println(new String(k.getRow()));
//            System.out.println(new String(k.getFamily()));
//            System.out.println(new String(k.getQualifier()));
//            System.out.println(k.getTimestamp());
//            System.out.println(new String(k.getValue()));
//        }
        //Cell截串---------------------------------------------------
//        for (Cell c : result.rawCells()) {
//            //获取一行
//            //       row-4fam1col-1  aP�@�value41
//            String row = new String(c.getValueArray());
//            System.out.println(row);
//            //获取偏移量，获取长度
//            //行键
//            System.out.println(row.substring(c.getRowOffset(),
//                    c.getRowOffset() + c.getRowLength()));
//            //列族
//            System.out.println(row.substring(c.getFamilyOffset(),
//                    c.getFamilyOffset() + c.getFamilyLength()));
//            //列名
//            System.out.println(row.substring(c.getQualifierOffset(),
//                    c.getQualifierOffset() + c.getQualifierLength()));
//            //时间戳
//            System.out.println(c.getTimestamp());
//            //值
//            System.out.println(row.substring(c.getValueOffset(),
//                    c.getValueOffset() + c.getValueLength()));
//        }
        //多版本查询-----------------------------------------
        Get get1 = new Get(Bytes.toBytes("row-1"));
        //可不限定列族
        //get1.addFamily(Bytes.toBytes("fam1"));
        //-1-
        //get1.setMaxVersions();
        //-2-:结果不会超过设定
        get1.setMaxVersions(6);
        Result result1 = table.get(get1);
        for (Cell c : result1.rawCells()) {
            System.out.println(new String(CellUtil.cloneFamily(c)));
            System.out.println(new String(CellUtil.cloneQualifier(c)));
            System.out.println(new String(CellUtil.cloneValue(c)));
        }
        table.close();
        conn.close();
    }
}
