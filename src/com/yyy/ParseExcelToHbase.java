package com.yyy;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ParseExcelToHbase {
	private static final String HBASER_MASTER_IP = "192.168.232.128";
	private static final String HBASER_MASTER_PORT = "60000";
	private static final String HBASER_ZOOKEEPER_PORT = "2181";
	private static final String TABLE_NAME = "TestTable";

	private Configuration configuration;
	private static final String FILE_NAME = "test.xls";
	private static String[] strHeaders={"des","quan","unit","brand"};

	public ParseExcelToHbase() {
		super();
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", HBASER_ZOOKEEPER_PORT);
		configuration.set("hbase.zookeeper.quorum", HBASER_MASTER_IP);
		configuration.set("hbase.master", HBASER_MASTER_IP + ":" + HBASER_MASTER_PORT);
	}

	public static void main(String[] args) {
		ParseExcelToHbase pe = new ParseExcelToHbase();
		//pe.parse();
		pe.createTable(TABLE_NAME, strHeaders);
	}

	public void parse() {
		createTable(TABLE_NAME,strHeaders);
		
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
			for (int i = 0; i < sheet.getRows(); i++) {
				cells = sheet.getRow(i);
				if (!isChineseChar(cells[0].getContents()) && !cells[0].equals("")) {
					parseRowAndPut(cells);
				}
			}

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

	public void parseRowAndPut(Cell[] cells) {
		String strDes = cells[0].getContents();
		String strQuan = cells[1].getContents();
		String strUnit = cells[2].getContents();
		System.out.println(strHeaders[0] + "-->" + strDes + "	" + strHeaders[1] + "-->" + strQuan);
		System.out.println(strHeaders[0] + "-->" + strDes + "	" + strHeaders[2] + "-->" + strUnit);
		for (int i = 3; i < 8; i++) {
			if (!cells[i].getContents().equals("")) {
				String strNum = cells[i].getContents();
				System.out
						.println(strHeaders[0] + "-->" + strDes + "	" + "brand" + ":" + strHeaders[i] + "-->" + strNum);
			}
		}

	}

	public void createTable(String tableName, String[] strColumn) {
		System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {// 如果存在要创建的表，那么先删除，再创建
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
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

	private static boolean isChineseChar(String str) {
		boolean temp = false;
		Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
		Matcher m = p.matcher(str);
		if (m.find()) {
			temp = true;
		}
		return temp;
	}
}