package hbaseTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class TableDesc {

	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin =conn.getAdmin();
		TableName tableName = TableName.valueOf("tablename");
		HTableDescriptor desc = admin.getTableDescriptor(tableName);
		System.out.println(desc);
		admin.close();
		conn.close();
	}
}
