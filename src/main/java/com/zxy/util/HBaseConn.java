package com.zxy.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConn {

    private static final HBaseConn INSTANCE = new HBaseConn();
    private static Configuration configuration;
    private static Connection connection;

    private HBaseConn() {
        try {
            System.setProperty("hadoop.home.dir", "C:\\dev\\hadoop-3.1.2");
            if (configuration == null) {
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","master");  //hbase 服务地址
                configuration.set("hbase.zookeeper.property.clientPort","2181"); //端口号
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Connection getConnection() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHBaseConn() {
        return INSTANCE.getConnection();
    }

    public static Table getTable(String tableName) throws IOException {
        return INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    public static void closeConn() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}