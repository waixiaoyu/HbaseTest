package com.yyy;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ExcelToMysql {
	private static final String FILE_NAME = "test.xls";
	private static final String TABLE_NAME = "DATA";
	private Connection conn = null;

	public ExcelToMysql() {
		super();
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // 动态加载mysql驱动
		this.conn = MysqlUtils.getConnection();
	}

	public static void main(String[] args) {
		ExcelToMysql etm = new ExcelToMysql();
		etm.createTable();
		etm.insert();
	}

	public void createTable() {
		if (conn == null) {
			conn = MysqlUtils.getConnection();
		}
		try {
			Statement stmt = conn.createStatement();
			if (!conn.getMetaData().getTables(null, null, TABLE_NAME, null).next()) {
				String sql = "CREATE TABLE " + TABLE_NAME
						+ " (`id` INT KEY NOT NULL AUTO_INCREMENT ,des CHAR(20),quantity CHAR(10),unit CHAR(10),COM CHAR(10),HeZhong CHAR(10), TCL CHAR(10), HuaDong CHAR(10),LQ CHAR(10),total CHAR(10))";
				int result = stmt.executeUpdate(sql);
				if (result != -1) {
					System.out.println("创建数据表成功");
				}
			}
		} catch (SQLException e) {
			System.out.println("MySQL操作错误");
			e.printStackTrace();
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn = null;
		}

	}

	public void insert() {
		if (conn == null) {
			conn = MysqlUtils.getConnection();
		}
		Sheet sheet;
		Workbook book;
		Cell[] cells;
		try {
			// t.xls为要读取的excel文件名
			book = Workbook.getWorkbook(new File(FILE_NAME));

			// 获得第一个工作表对象(ecxel中sheet的编号从0开始,0,1,2,3,....)
			sheet = book.getSheet(0);
			// 获取put数组，提高put效率
			for (int i = 0; i < 395; i++) {
				cells = sheet.getRow(i);
				if (!ExcelToHbase.isChineseChar(cells[0].getContents()) && !cells[0].getContents().equals("")) {
					parseRowAndPut(cells);
				}
			}
			book.close();
		} catch (Exception e) {
			System.out.println(e);
		} finally {
			try {
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn = null;
		}
	}

	public void parseRowAndPut(Cell[] cells) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement(
				"INSERT INTO data ( des, quantity, unit, COM, HeZhong, TCL, HuaDong, LQ,total)VALUES ( ?, ?, ?, ?, ?, ?, ?, ?,?)");

		for (int i = 0; i < 9; i++) {
			pstmt.setString(i + 1, cells[i].getContents());
		}
		pstmt.execute();
	}

}