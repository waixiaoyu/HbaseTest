package com.yyy.dao.oracletohbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.yyy.utils.HBaseUtils;
import com.yyy.utils.OracleUtils;

public class OracleToHbase {
	private static final String HBASE_TABLE_NAME = "bas_sku";

	private static final String dbUrl = "jdbc:oracle:thin:@120.24.53.12:1521:orcl";
	private static final String USERNAME = "WMS_USER";
	private static final String PASSWORD = "WMS_USER";

	private static String[] strColumns = { "cid", "sku", "f", "des","pid" };

	private Configuration configuration;
	private Connection conn;

	public OracleToHbase() {
		super();
		configuration = HBaseUtils.getConfiguration();
		conn = OracleUtils.getConnection(dbUrl, USERNAME, PASSWORD);
	}

	public static void main(String[] args) {
		OracleToHbase pe = new OracleToHbase();
		//pe.testDb("BAS_SKU");
		pe.parse("BAS_SKU");
		pe.createTable(HBASE_TABLE_NAME, strColumns);
	}

	public void testDb(String tablename) {
		ResultSet rs = search(tablename);
		if (rs != null) {
			try {
				while (rs.next()) {
					System.out.println(rs.getString(2));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("rs is null");
		}
	}

	public void parse(String tablename) {
		createTable(HBASE_TABLE_NAME, strColumns);
		// 获取put数组，提高put效率
		List<Put> lPuts = new ArrayList<Put>();
		ResultSet rs = search(tablename);
		if (rs != null) {
			try {
				while (rs.next()) {
					lPuts.add(getPutData(rs));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			System.out.println("rs is null");
		}
		HTable table = null;
		try {
			table = new HTable(configuration, HBASE_TABLE_NAME);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			table.put(lPuts);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public ResultSet search(String tablename) {
		if (conn == null) {
			conn = OracleUtils.getConnection();
		}
		PreparedStatement pstmt;
		try {
			// 在oracle中需必须将表名加上双引号
			pstmt = conn.prepareStatement("select * from \"" + tablename + "\"");
			ResultSet rs = pstmt.executeQuery();
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Put getPutData(ResultSet rs) throws SQLException {
		String rowKey=rs.getString(1)+","+rs.getString(2);
		// 设定rowkey
		Put put = new Put(rowKey.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		// 设定每个family和对应的value
		put.addColumn(strColumns[1].getBytes(), null, strQuan.getBytes());// 本行数据的第一列
		put.addColumn(strColumns[2].getBytes(), null, strUnit.getBytes());// 本行数据的第一列

		for (int i = 5; i < 10; i++) {
			if (!rs.getString(3).equals("")) {
				String strNum = rs.getString(3);
				// 设置family和qualifier
				put.addColumn(strColumns[3].getBytes(), rs.getMetaData().getColumnName(i).getBytes(),
						strNum.getBytes());// 本行数据的第一列
				// System.out.println(strRowKey + "-->" + strDes + " " + "brand"
				// + ":" + strHeaders[i] + "-->" + strNum);
			}
		}
		return put;
	}

	public void createTable(String tableName, String[] strColumn) {
		System.out.println("start create table ......");
		try {

			HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				// hBaseAdmin.disableTable(tableName);
				// hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
				return;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
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