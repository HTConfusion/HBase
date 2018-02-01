package Test1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class One {

	public Connection getConnection() throws IOException {
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "master,slave1");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		Connection conn = ConnectionFactory.createConnection(config);
		return conn;
	}

	// 任务1：请在已经搭建好的HBase环境中，建立一张表，表名“GoodsOrders”，带有2个列族｛f1,f2｝
	public void createTable() throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf("GoodsOrders");
		HTableDescriptor desc = new HTableDescriptor(tableName);
		HColumnDescriptor cd1 = new HColumnDescriptor(Bytes.toBytes("f1"));
		HColumnDescriptor cd2 = new HColumnDescriptor(Bytes.toBytes("f2"));
		desc.addFamily(cd1);
		desc.addFamily(cd2);
		admin.createTable(desc);
		admin.close();
		conn.close();
	}

	// 任务2：编写代码，实现新建立表“GoodsOrders”的表结构的查询
	// 任务3：编写代码，实现HBase库中所有表的表结构的查询
	public void tableDescriptor(boolean flag) throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		if (flag) {// 查询单个
			TableName tableName = TableName.valueOf("GoodsOrders");
			HTableDescriptor desc = admin.getTableDescriptor(tableName);
			System.out.println(desc);
		} else {// 查询多个
			HTableDescriptor[] descs = admin.listTables();
			for (HTableDescriptor h : descs) {
				System.out.println(h);
			}
		}
		admin.close();
		conn.close();
	}

	// 任务4：编写代码，实现禁用、启用和检查表的状态的功能----?
	public void disable_enable_status(String name, Chooose choose) throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf(name);
		if (admin.tableExists(tableName)) {
			switch (choose) {
			case disable:
				if (admin.isTableEnabled(tableName)) {
					admin.disableTable(tableName);
				}
				break;
			case enable:
				if (admin.isTableDisabled(tableName)) {
					admin.enableTable(tableName);
				}
				break;
			case status:
				System.out.println(admin.getTableDescriptor(tableName));
				break;
			default:
				break;
			}
		}
		admin.close();
		conn.close();
	}

	public static enum Chooose {
		disable, enable, status
	}

	// 任务5：编写代码，在现有表'GoodsOrders'中增加列族“f3”，并查询表结构看是否更改成功
	public void addFamily() throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf("GoodsOrders");
		HTableDescriptor desc = admin.getTableDescriptor(tableName);
		HColumnDescriptor cd = new HColumnDescriptor(Bytes.toBytes("f3"));
		desc.addFamily(cd);
		admin.modifyTable(tableName, desc);
		System.out.println(admin.getTableDescriptor(tableName));
		admin.close();
		conn.close();
	}

	// 任务6：编写代码，删除新增加的列族“f3”
	public void deleteFamily() throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf("GoodsOrders");
		HTableDescriptor desc = admin.getTableDescriptor(tableName);
		desc.removeFamily(Bytes.toBytes("f3"));
		admin.modifyTable(tableName, desc);
		admin.close();
		conn.close();
	}

	// 任务7：向表“GoodsOrders”中填加内容：行键：myrow-1 列：column=f1:HBase 值：1
	public void tablePut() throws IOException {
		Connection conn = getConnection();
		TableName tableName = TableName.valueOf("GoodsOrders");
		Table table = conn.getTable(tableName);
		Put put = new Put(Bytes.toBytes("myrow-1"));
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("HBase"), Bytes.toBytes(1));
		table.put(put);
		table.close();
		conn.close();
	}

	// 任务8：按析出的表格式，把其它没有插入的数据一起插入到表“GoodsOrders”中---？

	// 任务9：编写代码，删除表“GoodsOrders”
	public void tableDelete() throws IOException {
		Connection conn = getConnection();
		Admin admin = conn.getAdmin();
		TableName tableName = TableName.valueOf("GoodsOrders");
		if (admin.tableExists(tableName)) {
			if (admin.isTableEnabled(tableName)) {
				admin.disableTable(tableName);
			}
			admin.deleteTable(tableName);
		}
		admin.close();
		conn.close();
	}

	public static void main(String[] args) throws Exception {
		One o = new One();
		// o.createTable();
		// o.tableDescriptor(false);
		// o.disable_enable_status("GoodsOrders", Chooose.status);
		// o.addFamily();
		// o.deleteFamily();
		// o.tablePut();
		// o.tableDelete();
	}
}
