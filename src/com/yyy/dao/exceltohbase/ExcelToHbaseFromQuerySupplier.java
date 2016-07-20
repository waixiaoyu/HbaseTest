package com.yyy.dao.exceltohbase;

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

public class ExcelToHbaseFromQuerySupplier {
	private static final String TABLE_NAME = "querysupplier";
	private static final String FILE_NAME = "F:\\testdata\\querysupplier.xls";

	private static String[] strFamilys = { "cname", "sname", "price" };

	private Configuration configuration;

	public ExcelToHbaseFromQuerySupplier() {
		super();
		configuration = HBaseUtils.getConfiguration();
	}

	public static void main(String[] args) throws IOException {
		ExcelToHbaseFromQuerySupplier pe = new ExcelToHbaseFromQuerySupplier();
		//HBaseUtils.createTable(TABLE_NAME, strFamilys);
		//pe.deleteTable(TABLE_NAME);
		 pe.parseExcel();
		// pe.createTable(TABLE_NAME, strFamilys);
	}

	public void parseExcel() throws IOException {
		createTable(TABLE_NAME, strFamilys);

		Sheet sheet;
		Workbook book;
		Cell[] cells;
		try {
			// t.xlsΪҪ��ȡ��excel�ļ���
			book = Workbook.getWorkbook(new File(FILE_NAME));

			// ��õ�һ�����������(ecxel��sheet�ı�Ŵ�0��ʼ,0,1,2,3,....)
			sheet = book.getSheet(0);
			// ��ȡput���飬���putЧ��
			List<Put> lPuts = new ArrayList<Put>();

			for (int i = 1; i < sheet.getRows()-1; i++) {
				lPuts.add(parseRowAndPut(sheet.getRow(i)));
			}

			HTable table = null;
			try {
				table = new HTable(configuration, TABLE_NAME);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			table.put(lPuts);
			System.out.println("insert!");

			book.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public Put parseRowAndPut(Cell[] cells) {
		Put put = new Put((cells[0].getContents() + "," + cells[2].getContents()).getBytes());// һ��PUT����һ�����ݣ���NEWһ��PUT��ʾ�ڶ�������,ÿ��һ��Ψһ��ROWKEY���˴�rowkeyΪput���췽���д����ֵ
		// �趨ÿ��family�Ͷ�Ӧ��value
		put.addColumn(strFamilys[0].getBytes(), null, cells[0].getContents().getBytes());// �������ݵĵ�һ��
		put.addColumn(strFamilys[1].getBytes(), null, cells[1].getContents().getBytes());// �������ݵĵ�һ��

		put.addColumn(strFamilys[2].getBytes(), "ori".getBytes(), cells[2].getContents().getBytes());
		put.addColumn(strFamilys[2].getBytes(), "car".getBytes(), cells[3].getContents().getBytes());
		put.addColumn(strFamilys[2].getBytes(), "inv".getBytes(), cells[4].getContents().getBytes());

		return put;
	}

	public void deleteTable(String tableName) throws IOException {
		System.out.println("start delete table ......");

		HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
		if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
			System.out.println(tableName + " is deleted");
		} else {
			System.out.println(tableName + " is not existed");
		}
	}

	public void createTable(String tableName, String[] strFamilys) throws IOException {
		System.out.println("start create table ......");

		HBaseAdmin hBaseAdmin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
		if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���
			// hBaseAdmin.disableTable(tableName);
			// hBaseAdmin.deleteTable(tableName);
			System.out.println(tableName + " is exist");
			return;
		}
		HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
		for (String string : strFamilys) {
			tableDescriptor.addFamily(new HColumnDescriptor(string));
		}
		hBaseAdmin.createTable(tableDescriptor);

		System.out.println("end create table ......");
	}
}