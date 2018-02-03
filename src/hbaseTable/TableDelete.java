package hbaseTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * 表数据删除
 * Created by LHT on 2018/2/3
 */
public class TableDelete {
    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "master,slave1");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = ConnectionFactory.createConnection(config);
        TableName tableName = TableName.valueOf("tablename");
        Table table = conn.getTable(tableName);
        //创建实例
        Delete delete = new Delete(Bytes.toBytes("row-3"));
        //添加描述
        delete.addFamily(Bytes.toBytes("fam1"));
        //实施
        table.delete(delete);
        table.close();
        conn.close();
    }
}
