package com.yyy;

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

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ExcelToHbase {
	private static final String TABLE_NAME = "testtable";
	private static final String FILE_NAME = "test.xls";

	private static String[] strHeaders;
	private static String[] strColumns = { "des", "quan", "unit", "brand" };

	private Configuration configuration;

	public ExcelToHbase() {
		super();
		configuration = HBaseUtils.getConfiguration();
	}

	public static void main(String[] args) {
		ExcelToHbase pe = new ExcelToHbase();
		pe.parse();
		// pe.createTable(TABLE_NAME, strHeaders);
	}

	
	public void parse() {
		createTable(TABLE_NAME, strColumns);

		Sheet sheet;
		Workbook book;
		Cell[] cells;
		try {
			// t.xlsΪҪ��ȡ��excel�ļ���
			book = Workbook.getWorkbook(new File(FILE_NAME));

			// ��õ�һ�����������(ecxel��sheet�ı�Ŵ�0��ʼ,0,1,2,3,....)
			sheet = book.getSheet(0);
			// ��ȡ��ͷ
			getHeaders(sheet);
			// ��ȡput���飬���putЧ��
			List<Put> lPuts = new ArrayList<Put>();
			for (int i = 0; i < 395; i++) {
				cells = sheet.getRow(i);
				if (!isChineseChar(cells[0].getContents()) && !cells[0].getContents().equals("")) {
					System.out.println(i);
					lPuts.add(parseRowAndPut(cells));
				}
			}
			
			HTable table = null;
			try {
				table = new HTable(configuration, TABLE_NAME);
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			table.put(lPuts);

			book.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void getHeaders(Sheet sheet) {
		Cell[] cells = sheet.getRow(0);
		strHeaders = new String[cells.length];
		for (int i = 0; i < cells.length; i++) {
			strHeaders[i] = cells[i].getContents();
		}
	}

	public Put parseRowAndPut(Cell[] cells) {

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

	public static boolean isChineseChar(String str) {
		boolean temp = false;
		Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
		Matcher m = p.matcher(str);
		if (m.find()) {
			temp = true;
		}
		return temp;
	}
}