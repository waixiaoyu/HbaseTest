package com.yyy.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class OracleUtils {
	private static Connection conn = null;
	private static String USERNAME = "sys as sysdba";
	private static String PASSWORD = "123123";

	public static void main(String[] args) {
		OracleUtils.getConnection("jdbc:oracle:thin:@120.24.53.12:1521:orcl", "WMS_USER", "WMS_USER");
	}

	// ȡ������,Ĭ�ϱ�������
	public static Connection getConnection() {
		if (conn == null) {
			try {
				String sDriverName = "oracle.jdbc.driver.OracleDriver";
				String sDBUrl = "jdbc:oracle:thin:@localhost:1521:test";
				Class.forName(sDriverName);
				conn = DriverManager.getConnection(sDBUrl, USERNAME, PASSWORD);
				if (conn != null) {
					System.out.println("oracle ���ݿ����ӳɹ���");
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

	// Զ������
	public static Connection getConnection(String sDBUrl, String username, String password) {
		USERNAME = username;
		PASSWORD = password;
		if (conn == null) {
			try {
				String sDriverName = "oracle.jdbc.driver.OracleDriver";
				Class.forName(sDriverName);
				conn = DriverManager.getConnection(sDBUrl, USERNAME, PASSWORD);
				if (conn != null) {
					System.out.println("���ݿ����ӳɹ���");
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