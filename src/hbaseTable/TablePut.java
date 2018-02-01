package hbaseTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class TablePut {

	public static void main(String[] args) throws Exception {
		// 1.获取资源
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// 2.创建连接
		// HTable tb = new HTable(conf, "tablename");//过时
		Connection conn = ConnectionFactory.createConnection(conf);
		// 3.表的管理Table
		TableName tableName = TableName.valueOf("tablename");
		Table table = conn.getTable(tableName);
		//插入单条数据=============================================
		// 4.建立插入信息的描述Put
		// 输入行键
		Put put = new Put(Bytes.toBytes("row-1"));
		// family列族, qualifier列名, ts时间戳, value值
		// 过时
		//put.add(Bytes.toBytes("fam1"), Bytes.toBytes("col-1"), Bytes.toBytes("value1"));
		//put.add(Bytes.toBytes("fam2"), Bytes.toBytes("col-2"), Bytes.toBytes("value2"));
		
		put = new Put(Bytes.toBytes("row-2"));
		put.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col-1"), Bytes.toBytes("value3"));
		put.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col-2"), Bytes.toBytes("value4"));
		//5.向Table提交描述，并Table提交--有事务管理(仅限于一行)
		table.put(put);
		//插入多条数据=============================================
		List<Put> puts = new ArrayList<>();
		Put put1 = new Put(Bytes.toBytes("row-4"));
		put1.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col-1"), Bytes.toBytes("value41"));
		put1.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col-2"), Bytes.toBytes("value42"));
		
		Put put2 = new Put(Bytes.toBytes("row-5"));//列族fam666不存在
		put2.addColumn(Bytes.toBytes("fam666"), Bytes.toBytes("col-1"), Bytes.toBytes("value51"));
		put2.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col-2"), Bytes.toBytes("value52"));
		
		Put put3 = new Put(Bytes.toBytes("row-6"));
		put3.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("col-1"), Bytes.toBytes("value61"));
		put3.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("col-2"), Bytes.toBytes("value62"));
		
		puts.add(put1);
		puts.add(put2);
		puts.add(put3);
		table.put(puts);//各别行出错，不影响整体数据插入
		//6.关闭连接
		table.close();
		conn.close();
	}

}
