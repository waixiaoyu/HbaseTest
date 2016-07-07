package com.yyy.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class SQLServerUtils {
	private static Connection conn = null;
	private static final String USERNAME = "sa";
	private static final String PASSWORD = "123123";

	public static void main(String[] args) {
		SQLServerUtils.getConnection();
	}

	// 取得连接
	public static Connection getConnection() {
		if (conn == null) {
			try {
				String sDriverName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
				// String sDBUrl
				// ="jdbc:sqlserver://192.168.0.74;databaseName=wakeup";
				String sDBUrl = "jdbc:sqlserver://192.168.3.28:1433;databaseName=test";
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