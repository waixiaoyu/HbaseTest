package com.yyy.dao;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.yyy.utils.HBaseUtils;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class MysqlToHbase {
	private static final String TABLE_NAME = "testtable";
	private static final String FILE_NAME = "test.xls";

	private static String[] strHeaders;
	private static String[] strColumns = { "des", "quan", "unit", "brand" };

	private Configuration configuration;

	public MysqlToHbase() {
		super();
		configuration = HBaseUtils.getConfiguration();
	}

	public static void main(String[] args) {
		MysqlToHbase pe = new MysqlToHbase();
		
		// pe.createTable(TABLE_NAME, strHeaders);
	}
	public Put putData(Cell[] cells) {

		String strRowKey = strColumns[0];
		String strDes = cells[0].getContents();
		String strQuan = cells[1].getContents();
		String strUnit = cells[2].getContents();
		// �趨rowkey
		Put put = new Put(strDes.getBytes());// һ��PUT����һ�����ݣ���NEWһ��PUT��ʾ�ڶ�������,ÿ��һ��Ψһ��ROWKEY���˴�rowkeyΪput���췽���д����ֵ
		// �趨ÿ��family�Ͷ�Ӧ��value
		put.addColumn(strColumns[1].getBytes(), null, strQuan.getBytes());// �������ݵĵ�һ��
		System.out.println(strRowKey + "-->" + strDes + "	" + strHeaders[1] + "-->" + strQuan);
		put.addColumn(strColumns[2].getBytes(), null, strUnit.getBytes());// �������ݵĵ�һ��
		System.out.println(strRowKey + "-->" + strDes + "	" + strHeaders[2] + "-->" + strUnit);

		for (int i = 3; i < 8; i++) {
			if (!cells[i].getContents().equals("")) {
				String strNum = cells[i].getContents();
				// ����family��qualifier
				put.addColumn(strColumns[3].getBytes(), strHeaders[i].getBytes(), strNum.getBytes());// �������ݵĵ�һ��
				System.out.println(strRowKey + "-->" + strDes + "	" + "brand" + ":" + strHeaders[i] + "-->" + strNum);
			}
		}
		return put;
	}

	public void createTable(String tableName, String[] strColumn) {
		System.out.println("start create table ......");
		try {

			HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
			if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���
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