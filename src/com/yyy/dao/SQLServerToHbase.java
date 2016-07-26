package com.yyy.dao;

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
import com.yyy.utils.SQLServerUtils;

public class SQLServerToHbase {
	private static final String TABLE_NAME = "PLT_KC";

	private static String[] strColumns = { "CAI_EIE_LIST", "CAI_CON_DCRT", "CAI_CON_LIST", "BI_TERMS_CON" };

	private static final String FAMILY = "CAI_EIE_LIST";

	private Configuration configuration;
	private Connection conn;

	public SQLServerToHbase() {
		super();
		configuration = HBaseUtils.getConfiguration();
		conn = SQLServerUtils.getConnection();
	}

	public static void main(String[] args) {
		SQLServerToHbase pe = new SQLServerToHbase();
		pe.parse2();
		// pe.createTable(TABLE_NAME, strHeaders);
	}

	public void parse() {
		createTable(TABLE_NAME, strColumns);
		// 获取put数组，提高put效率
		List<Put> lPuts = new ArrayList<Put>();
		ResultSet rs = search();
		if (rs != null) {
			try {
				while (rs.next()) {
					lPuts.add(getPutData2(rs));
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
			table = new HTable(configuration, TABLE_NAME);
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

	public void parse2() {
		createTable(TABLE_NAME, strColumns);
		// 获取put数组，提高put效率
		List<Put> lPuts = new ArrayList<Put>();
		ResultSet rs = search2();
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
			table = new HTable(configuration, TABLE_NAME);
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

	public ResultSet search() {
		if (conn == null) {
			conn = SQLServerUtils.getConnection();
		}
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(
					"select * from CAI_EIE_LIST");
			ResultSet rs = pstmt.executeQuery();
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public ResultSet search2() {
		if (conn == null) {
			conn = SQLServerUtils.getConnection();
		}
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement(
					"select CAI_CON_DCRT.*,CAI_EIE_LIST.EL_IN,CAI_EIE_LIST.NEWID from CAI_CON_DCRT FULL JOIN CAI_EIE_LIST on CAI_CON_DCRT.D_CON_NEWID=CAI_EIE_LIST.NEWID");
			ResultSet rs = pstmt.executeQuery();
			return rs;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public Put getPutData(ResultSet rs) throws SQLException {

		// 设定rowkey
		Put put = new Put((rs.getString("EL_IN") + "," + rs.getString("NEWID")).getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		// 设定每个family和对应的value
		put.addColumn(FAMILY.getBytes(), "CZY".getBytes(),
				rs.getString("CZY") == null ? null : rs.getString("CZY").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EIE_NEWID".getBytes(),
				rs.getString("EIE_NEWID") == null ? null : rs.getString("EIE_NEWID").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_AM".getBytes(),
				rs.getString("EL_AM") == null ? null : rs.getString("EL_AM").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_CURR".getBytes(),
				rs.getString("EL_CURR") == null ? null : rs.getString("EL_CURR").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_CUSTOM".getBytes(),
				rs.getString("EL_CUSTOM") == null ? null : rs.getString("EL_CUSTOM").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_DG".getBytes(),
				rs.getString("EL_DG") == null ? null : rs.getString("EL_DG").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_ETYPE".getBytes(),
				rs.getString("EL_ETYPE") == null ? null : rs.getString("EL_ETYPE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EIE_NEWID".getBytes(),
				rs.getString("EIE_NEWID") == null ? null : rs.getString("EIE_NEWID").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_LN".getBytes(),
				rs.getString("EL_LN") == null ? null : rs.getString("EL_LN").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_LV".getBytes(),
				rs.getString("EL_LV") == null ? null : rs.getString("EL_LV").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_OBPORT".getBytes(),
				rs.getString("EL_OBPORT") == null ? null : rs.getString("EL_OBPORT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_PLT".getBytes(),
				rs.getString("EL_PLT") == null ? null : rs.getString("EL_PLT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_QU".getBytes(),
				rs.getString("EL_QU") == null ? null : rs.getString("EL_QU").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_REMARKS".getBytes(),
				rs.getString("EL_REMARKS") == null ? null : rs.getString("EL_REMARKS").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_RQ".getBytes(),
				rs.getString("EL_RQ") == null ? null : rs.getString("EL_RQ").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_SHPORT".getBytes(),
				rs.getString("EL_SHPORT") == null ? null : rs.getString("EL_SHPORT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_SUP".getBytes(),
				rs.getString("EL_SUP") == null ? null : rs.getString("EL_SUP").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_TYPE".getBytes(),
				rs.getString("EL_TYPE") == null ? null : rs.getString("EL_TYPE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_USDRATE".getBytes(),
				rs.getString("EL_USDRATE") == null ? null : rs.getString("EL_USDRATE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_VOL".getBytes(),
				rs.getString("EL_VOL") == null ? null : rs.getString("EL_VOL").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "EL_WEIGHT".getBytes(),
				rs.getString("EL_WEIGHT") == null ? null : rs.getString("EL_WEIGHT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "STATUS".getBytes(),
				rs.getString("STATUS") == null ? null : rs.getString("STATUS").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "CZWD".getBytes(),
				rs.getString("CZWD") == null ? null : rs.getString("CZWD").getBytes());// 本行数据的第一列

		return put;
	}

	public Put getPutData2(ResultSet rs) throws SQLException {

		System.out.println(rs.getString("D_CON_NEWID"));
		System.out.println(rs.getString("EL_IN"));
		if (rs.getString("D_CON_NEWID") == null || rs.getString("EL_IN") == null) {
			return null;
		}
		// 设定rowkey
		Put put = new Put((rs.getString("EL_IN") + "," + rs.getString("D_CON_NEWID")).getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		// 设定每个family和对应的value
		put.addColumn(FAMILY.getBytes(), "CZY".getBytes(),
				rs.getString("CZY") == null ? null : rs.getString("CZY").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_ADOC".getBytes(),
				rs.getString("D_ADOC") == null ? null : rs.getString("D_ADOC").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_APPNU".getBytes(),
				rs.getString("D_APPNU") == null ? null : rs.getString("D_APPNU").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_CN".getBytes(),
				rs.getString("D_CN") == null ? null : rs.getString("D_CN").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_CONV".getBytes(),
				rs.getString("D_CONV") == null ? null : rs.getString("D_CONV").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_COS".getBytes(),
				rs.getString("D_COS") == null ? null : rs.getString("D_COS").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_DDATE".getBytes(),
				rs.getString("D_DDATE") == null ? null : rs.getString("D_DDATE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_ETYPE".getBytes(),
				rs.getString("D_ETYPE") == null ? null : rs.getString("D_ETYPE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_FREIGHT".getBytes(),
				rs.getString("D_FREIGHT") == null ? null : rs.getString("D_FREIGHT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_HSNO".getBytes(),
				rs.getString("D_HSNO") == null ? null : rs.getString("D_HSNO").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_ICP".getBytes(),
				rs.getString("D_ICP") == null ? null : rs.getString("D_ICP").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_IDATE".getBytes(),
				rs.getString("D_IDATE") == null ? null : rs.getString("D_IDATE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_IPORT".getBytes(),
				rs.getString("D_IPORT") == null ? null : rs.getString("D_IPORT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_LICENSE".getBytes(),
				rs.getString("D_LICENSE") == null ? null : rs.getString("D_LICENSE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_NUMBER".getBytes(),
				rs.getString("D_NUMBER") == null ? null : rs.getString("D_NUMBER").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_PERAPP".getBytes(),
				rs.getString("D_PERAPP") == null ? null : rs.getString("D_PERAPP").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_PORTN".getBytes(),
				rs.getString("D_PORTN") == null ? null : rs.getString("D_PORTN").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_PREMIUMD_YEFEE".getBytes(),
				rs.getString("D_PREMIUMD_YEFEE") == null ? null : rs.getString("D_PREMIUMD_YEFEE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_PSIHIPMENT".getBytes(),
				rs.getString("D_PSIHIPMENT") == null ? null : rs.getString("D_PSIHIPMENT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_REACH".getBytes(),
				rs.getString("D_REACH") == null ? null : rs.getString("D_REACH").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_REMARKS".getBytes(),
				rs.getString("D_REMARKS") == null ? null : rs.getString("D_REMARKS").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_SHIPMENT".getBytes(),
				rs.getString("D_SHIPMENT") == null ? null : rs.getString("D_SHIPMENT").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_SUMTAX".getBytes(),
				rs.getString("D_SUMTAX") == null ? null : rs.getString("D_SUMTAX").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_TAXR".getBytes(),
				rs.getString("D_TAXR") == null ? null : rs.getString("D_TAXR").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_TRADE".getBytes(),
				rs.getString("D_TRADE") == null ? null : rs.getString("D_TRADE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_TRAN".getBytes(),
				rs.getString("D_TRAN") == null ? null : rs.getString("D_TRAN").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_TURN".getBytes(),
				rs.getString("D_TURN") == null ? null : rs.getString("D_TURN").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_UNIFIEDNO".getBytes(),
				rs.getString("D_UNIFIEDNO") == null ? null : rs.getString("D_UNIFIEDNO").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "D_USE".getBytes(),
				rs.getString("D_USE") == null ? null : rs.getString("D_USE").getBytes());// 本行数据的第一列
		put.addColumn(FAMILY.getBytes(), "SL_CURR".getBytes(),
				rs.getString("SL_CURR") == null ? null : rs.getString("SL_CURR").getBytes());// 本行数据的第一列
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