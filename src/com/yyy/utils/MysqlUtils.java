package com.yyy.utils;

import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlUtils {
	private static final String url = "jdbc:mysql://localhost:3306/test?"
			+ "user=root&password=123123&useUnicode=true&characterEncoding=UTF8";

	public static Connection getConnection() {
		Connection con = null;
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 注册数据库驱动
			// 定义连接数据库的url
			con = DriverManager.getConnection(url); // 获取数据库连接
			System.out.println("数据库连接成功！");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return con; // 返回一个连接
	}
}
