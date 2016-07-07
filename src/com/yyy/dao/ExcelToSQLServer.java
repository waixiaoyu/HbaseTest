package com.yyy.dao;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.yyy.utils.SQLServerUtils;

import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;

public class ExcelToSQLServer {
	private static final String FILE_NAME = "test.xls";
	private static final String TABLE_NAME = "DATA";
	private Connection conn = null;

	public ExcelToSQLServer() {
		super();
		this.conn = SQLServerUtils.getConnection();
	}

	public static void main(String[] args) {
		ExcelToSQLServer etm = new ExcelToSQLServer();
		etm.createTable();
		etm.insert();
		//etm.search();
	}

	public void createTable() {
		if (conn == null) {
			conn = SQLServerUtils.getConnection();
		}
		try {
			Statement stmt = conn.createStatement();
			if (!conn.getMetaData().getTables(null, null, TABLE_NAME, null).next()) {
				String sql = "CREATE TABLE " + TABLE_NAME
						+ " (id int IDENTITY (1,1) PRIMARY KEY ,des CHAR(20),quantity CHAR(10),unit CHAR(10),COM CHAR(10),HeZhong CHAR(10), TCL CHAR(10), HuaDong CHAR(10),LQ CHAR(10),total CHAR(10))";
				int result = stmt.executeUpdate(sql);
				if (result != -1) {
					System.out.println("创建数据表成功");
				}
			}
		} catch (SQLException e) {
			System.out.println("MySQL操作错误");
			e.printStackTrace();
		} finally {

		}

	}

	public void search() {
		if (conn == null) {
			conn = SQLServerUtils.getConnection();
		}
		PreparedStatement pstmt;
		try {
			pstmt = conn.prepareStatement("select * from data");
			ResultSet rs=pstmt.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void insert() {
		if (conn == null) {
			conn = SQLServerUtils.getConnection();
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
			System.out.println("插入结束！");
		} catch (Exception e) {
			System.out.println(e);
		} finally {

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