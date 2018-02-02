package hbaseTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 批量查询数据+过滤器
 * Created by LHT on 2018/1/31
 */
public class TableScan {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("tablename");
        Table table = conn.getTable(tableName);
        //1---------------------------------------------------
//        //输入不存在列也可比较运行，容错率高
//        Scan scan = new Scan();
//        //左闭右开
//        scan.setStartRow(Bytes.toBytes("row-1"));
//        scan.setStopRow(Bytes.toBytes("row-8"));
//        ResultScanner results = table.getScanner(scan);
//        for(Result r:results){
//            System.out.println(r);
//        }
        //2---------------------------------------------------
//        //构造器设定起止位置
//        Scan scan = new Scan(Bytes.toBytes("row-1"),Bytes.toBytes("row-8"));
//        ResultScanner result = table.getScanner(scan);
//        for (Result r : result) {
//            System.out.println(r);
//        }
        //3.过滤器---------------------------------------------
//        Scan scan = new Scan();
//        //第一参数：枚举参数--选择等于或大于等对比类型  第二参数：ByteArrayComparable的子类--选择阈值或正则等对比方法
//        //行过滤器
//        //CompareFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row-3")));
//        //值过滤器
//        CompareFilter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("1"));
//        //        //向Scan植入过滤器
//        scan.setFilter(filter);
//        ResultScanner result = table.getScanner(scan);
//        for (Result r : result) {
//            System.out.println(r);
//            for (Cell c : r.rawCells()) {
//                System.out.println(new String(CellUtil.cloneValue(c)));
//            }
//        }
        //4.多版本查询-----------------------------------------
        Scan scan = new Scan(Bytes.toBytes("row-1"), Bytes.toBytes("row-2"));
        scan.setMaxVersions();
        ResultScanner results = table.getScanner(scan);
        for (Result r : results) {
            System.out.println(r);
            for (Cell c : r.listCells()) {
                System.out.println(new String(CellUtil.cloneFamily(c)));
                System.out.println(new String(CellUtil.cloneQualifier(c)));
                System.out.println(new String(CellUtil.cloneValue(c)));
            }
        }
        table.close();
        conn.close();
    }
}
