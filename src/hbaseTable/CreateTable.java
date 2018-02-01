package hbaseTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

	public static void main(String[] args) throws Exception {
		// 1.获取系统资源
		// 旧版本
		// Configuration conf = new HBaseConfiguration();
		// 新版本
		Configuration conf = HBaseConfiguration.create();
		// 本地无zookeeper资源配置，因此需指定资源位置
		// 指定多个节点时，调取多个节点的资源，若其中一个或几个节点失效，不影响整体运行
		conf.set("hbase.zookeeper.quorum", "master,slave1");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		// 2.管理者
		// HBaseAdmin admin = new HBaseAdmin(conf);//过时
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// 3.表的描述：绘制表的结构
		TableName tableName = TableName.valueOf("tablename");
		// 过时
		//HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes("tablename"));
		// 新建表--创建新的描述
//		HTableDescriptor desc = new HTableDescriptor(tableName);
//		HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("fam1"));
//		HColumnDescriptor hcd2 = new HColumnDescriptor(Bytes.toBytes("fam2"));
//		desc.addFamily(hcd1);
//		desc.addFamily(hcd2);
//========================================================================================
		// 设置列族块大小
//		HColumnDescriptor hcd1 = new HColumnDescriptor(Bytes.toBytes("fam1"));
//		hcd1.setBlocksize(65536);
//========================================================================================	
		// 修改表结构--获取原有表描述--如：添加列族
		HTableDescriptor htd = admin.getTableDescriptor(tableName);
		HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("fam3"));
		htd.addFamily(hcd);
		// 4.将表结构提交至admin，admin建立表
		//admin.createTable(desc);
		// 修改列族
		// 最好先设置停用表，若表已停用，在此设置会报错
		// admin.disableTable(tableName);
		// admin.modifyColumn(tableName, hcd1);
		// admin.enableTable(tableName);
		
		//修改表结构
		admin.modifyTable(tableName, htd);
		
		// 删除表,需先设置表不可用
//		boolean ifTableExist = admin.tableExists(tableName);
//		if(ifTableExist){
//			if(admin.isTableEnabled(tableName)){
//				admin.disableTable(tableName);
//			}
//		}
//		admin.deleteTable(tableName);
		// 5.资源关闭：Admin conn,按顺序从里到外依次删除
		admin.close();
		conn.close();
	}
}
