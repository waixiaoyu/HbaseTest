package com.yyy;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ParseExcel {
	private static final String FILE_NAME = "test.xls";
	private String[] strHeaders;

	public static void main(String[] args) {
		ParseExcel pe = new ParseExcel();
		pe.parse();
	}

	public void parse() {
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
					parseRow(cells);
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

	public void parseRow(Cell[] cells) {
		String strDes = cells[0].getContents();
		String strQuan = cells[1].getContents();
		String strUnit = cells[2].getContents();
		System.out.println(strHeaders[0] + "-->" + strDes + "	" + strHeaders[1] + "-->" + strQuan);
		System.out.println(strHeaders[0] + "-->" + strDes + "	" + strHeaders[2] + "-->" + strUnit);
		for (int i = 3; i < 8; i++) {
			if (!cells[i].getContents().equals("")) {
				String strNum = cells[i].getContents();
				System.out.println(strHeaders[0] + "-->" + strDes + "	" + "brand" + ":" + strHeaders[i] + "-->" + strNum);
			}
		}

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