package com.yyy.dao;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

import com.yyy.utils.HBaseUtils;

public class HBaseDAO {
	public static void main(String[] args) throws IOException {
		HBaseDAO.get("orderdetail", "Yvonne Yvonne");
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

	public static synchronized void deleteTable(String tableName) throws IOException {
		HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
		if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
			System.out.println(tableName + " is detele....");
		}
	}

	public static synchronized String get(String tableName, String rowKey) throws IOException {
		Get get = new Get(rowKey.getBytes());
		HTable table = new HTable(HBaseUtils.getConfiguration(), tableName);// 获取表
		Result result = table.get(get);
		//System.out.println(new String(result.getValue("content".getBytes(), "count".getBytes())));
		return new String(result.getValue("content".getBytes(), "count".getBytes()));
	}
}
