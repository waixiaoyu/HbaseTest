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
			// t.xls为要读取的excel文件名
			book = Workbook.getWorkbook(new File(FILE_NAME));

			// 获得第一个工作表对象(ecxel中sheet的编号从0开始,0,1,2,3,....)
			sheet = book.getSheet(0);
			// 获取表头
			getHeaders(sheet);
			// 获取put数组，提高put效率
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
		// 设定rowkey
		Put put = new Put(strDes.getBytes());// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值
		// 设定每个family和对应的value
		put.addColumn(strColumns[1].getBytes(), null, strQuan.getBytes());// 本行数据的第一列
		System.out.println(strRowKey + "-->" + strDes + "	" + strHeaders[1] + "-->" + strQuan);
		put.addColumn(strColumns[2].getBytes(), null, strUnit.getBytes());// 本行数据的第一列
		System.out.println(strRowKey + "-->" + strDes + "	" + strHeaders[2] + "-->" + strUnit);

		for (int i = 3; i < 8; i++) {
			if (!cells[i].getContents().equals("")) {
				String strNum = cells[i].getContents();
				// 设置family和qualifier
				put.addColumn(strColumns[3].getBytes(), strHeaders[i].getBytes(), strNum.getBytes());// 本行数据的第一列
				System.out.println(strRowKey + "-->" + strDes + "	" + "brand" + ":" + strHeaders[i] + "-->" + strNum);
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