package com.yyy.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseUtils {

	private static final String HBASER_MASTER_IP = "192.168.3.201";
	private static final String HBASER_MASTER_PORT = "60000";
	private static final String QUORUM_IP = "192.168.3.201";
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
		if (conf == null) {
			conf = getConfiguration();
		}
		if (conn == null) {
			try {
				conn = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Hbase 连接成功!");
		return conn;
	}
}