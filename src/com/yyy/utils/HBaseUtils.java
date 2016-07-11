package com.yyy.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseUtils {

	private static final String HBASER_MASTER_IP = "192.168.3.128";
	private static final String HBASER_MASTER_PORT = "60000";
	private static final String QUORUM_IP = "192.168.3.128";
	private static final String CLIENTPORT = "2181";
	private static Configuration conf = null;
	private static Connection conn = null;

	/**
	 * 获取全局唯一的Configuration实例
	 * 
	 * @return
	 */
	public static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.quorum", QUORUM_IP);
			conf.set("hbase.zookeeper.property.clientPort", CLIENTPORT);
			conf.set("hbase.master", HBASER_MASTER_IP + ":" + HBASER_MASTER_PORT);
		}
		return conf;
	}

	/**
	 * 获取全局唯一的HConnection实例
	 * 
	 * @return
	 * @throws ZooKeeperConnectionException
	 */
	public static synchronized Connection getHConnection() {
		if (conn == null) {
			try {
				conn = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return conn;
	}

	public static synchronized void createTable(String tableName, String[] strColumn) {
		System.out.println("start create table ......");
		try {

			HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				// hBaseAdmin.disableTable(tableName);
				// hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
				return;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
			for (String string : strColumn) {
				tableDescriptor.addFamily(new HColumnDescriptor(string));
			}
			hBaseAdmin.createTable(tableDescriptor);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end create table ......");
	}
}