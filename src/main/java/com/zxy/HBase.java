package com.zxy;

import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
 
public class HBase {
	
	public static Configuration conf;
	public static Connection connection;
	public static Admin admin;
 
	public static void main(String[] args) throws IOException {
		System.setProperty("hadoop.home.dir", "C:\\dev\\hadoop-3.1.2\\");
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","master");  //hbase 服务地址
		// conf.set("hbase.zookeeper.property.clientPort","2181"); //端口号
		// conf.set("hbase.master", "master:16000");
		// conf.set("hbase.master", "master:600000");

		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
		
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table1"));
		table.addFamily(new HColumnDescriptor("group1")); //创建表时至少加入一个列组
		
		if(admin.tableExists(table.getTableName())){
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
 
}