package com.yyy.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlUtils {
	private static final String url = "jdbc:mysql://localhost:3306/test?"
			+ "user=root&password=123123&useUnicode=true&characterEncoding=UTF8";

	public static Connection getConnection() {
		Connection con = null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // ע�����ݿ�����
			// �����������ݿ��url
			con = DriverManager.getConnection(url); // ��ȡ���ݿ�����
			System.out.println("���ݿ����ӳɹ���");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con; // ����һ������
	}
}
