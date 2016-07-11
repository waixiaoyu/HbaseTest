package com.yyy.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class OracleUtils {
	private static Connection conn = null;
	private static final String USERNAME = "sys as sysdba";
	private static final String PASSWORD = "123123";

	public static void main(String[] args) {
		OracleUtils.getConnection();
	}

	// 取得连接
	public static Connection getConnection() {
		if (conn == null) {
			try {
				String sDriverName = "oracle.jdbc.driver.OracleDriver";
				String sDBUrl = "jdbc:oracle:thin:@localhost:1521:test";
				Class.forName(sDriverName);
				conn = DriverManager.getConnection(sDBUrl, USERNAME, PASSWORD);
				if (conn != null) {
					System.out.println("数据库连接成功！");
				} else {
					System.out.println("conn = null");

				}
			} catch (Exception ex) {
				// ex.printStackTrace();
				System.out.println(ex.getMessage());
			}
		}
		return conn;
	}
}