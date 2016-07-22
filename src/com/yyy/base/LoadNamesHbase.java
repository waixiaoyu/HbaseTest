package com.yyy.base;

import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import com.yyy.dao.HBaseDAO;
import com.yyy.utils.HBaseUtils;
import com.yyy.utils.LoadUtils;

public class LoadNamesHbase {
	public static final String OUTPUT_PATH = "C:/Users/Administrator/Desktop/name.txt";

	public static void main(String[] args) throws Exception {
		String[] header = { "rec" };
		HBaseDAO.createTable("order", header);
		HTable table = new HTable(HBaseUtils.getConfiguration(), "order");

		List<String> names = LoadUtils.readResource(OUTPUT_PATH);
		int nCount=0;
		for (String string : names) {
			String[] str = string.split(" ");
			Put put = new Put(str[0].getBytes());
			put.addColumn(header[0].getBytes(), null, str[1].getBytes());
			table.put(put);
			System.out.println(nCount++);
		}
		table.close();
	}
}
