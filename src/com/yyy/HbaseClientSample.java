package com.yyy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClientSample {

	private static final String HBASER_MASTER_IP = "192.168.232.128";
	private static final String HBASER_MASTER_PORT = "60000";
	private static final String HBASER_ZOOKEEPER_PORT = "2181";
	private static final String TABLE_NAME = "testtable";
	public static Configuration configuration;
	static {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", HBASER_ZOOKEEPER_PORT);
		configuration.set("hbase.zookeeper.quorum", HBASER_MASTER_IP);
		configuration.set("hbase.master", HBASER_MASTER_IP + ":" + HBASER_MASTER_PORT);
	}

	public static void main(String[] args) {
		// createTable(TABLE_NAME);
		// insertData(TABLE_NAME);
		// QueryAll(TABLE_NAME);
		// QueryByCondition1(TABLE_NAME);
		// QueryByCondition2(TABLE_NAME);
		// QueryByCondition3(TABLE_NAME);
		 deleteRow(TABLE_NAME,"des");
		// deleteColumn(TABLE_NAME, "445566bbbcccc", "column2");
	}

	/**
	 * ������
	 * 
	 * @param tableName
	 */
	public static void createTable(String tableName) {
		System.out.println("start create table ......");
		try {
			HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
			if (hBaseAdmin.tableExists(tableName)) {// �������Ҫ�����ı���ô��ɾ�����ٴ���
				hBaseAdmin.disableTable(tableName);
				hBaseAdmin.deleteTable(tableName);
				System.out.println(tableName + " is exist,detele....");
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			tableDescriptor.addFamily(new HColumnDescriptor("column1"));
			tableDescriptor.addFamily(new HColumnDescriptor("column2"));
			tableDescriptor.addFamily(new HColumnDescriptor("column3"));
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

	/**
	 * ��������
	 * 
	 * @param tableName
	 * @throws IOException
	 */
	public static void insertData(String tableName) {
		System.out.println("start insert data ......");
		HTable table = null;
		try {
			table = new HTable(configuration, tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Put put = new Put("778899bbbcccc".getBytes());// һ��PUT����һ�����ݣ���NEWһ��PUT��ʾ�ڶ�������,ÿ��һ��Ψһ��ROWKEY���˴�rowkeyΪput���췽���д����ֵ
		put.addColumn("column1".getBytes(), null, "aaa".getBytes());// �������ݵĵ�һ��
		put.addColumn("column2".getBytes(), null, "qqqqqqqnull".getBytes());// �������ݵĵ�����
		put.addColumn("column3".getBytes(), null, "www".getBytes());// �������ݵĵ�����
		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("end insert data ......");
	}

	/**
	 * ɾ��һ�ű�
	 * 
	 * @param tableName
	 */
	public static void dropTable(String tableName) {
		try {
			HBaseAdmin admin = new HBaseAdmin(configuration);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * ���� rowkeyɾ��һ����¼
	 * 
	 * @param tablename
	 * @param rowkey
	 */
	public static void deleteRow(String tablename, String rowkey) {
		try {
			HTable table = new HTable(configuration, tablename);
			List<Delete> list = new ArrayList<Delete>();
			Delete d1 = new Delete(rowkey.getBytes());
			list.add(d1);

			table.delete(list);
			System.out.println("ɾ���гɹ�!");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * ɾ����
	 * 
	 * @param tablename
	 * @param columnname
	 */
	public static void deleteColumn(String tablename, String rowkey, String columnname) {
		HTable table;
		try {
			table = new HTable(configuration, tablename);

			Delete deleteColumn = new Delete(rowkey.getBytes());
			deleteColumn.deleteFamily(columnname.getBytes());
			table.delete(deleteColumn);
			System.out.println("ɾ���гɹ�!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ��ѯ��������
	 * 
	 * @param tableName
	 */
	public static void QueryAll(String tableName) {
		HTable table = null;
		try {
			table = new HTable(configuration, tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			ResultScanner rs = table.getScanner(new Scan());
			for (Result r : rs) {
				System.out.println("rowkey:" + new String(r.getRow()));
				for (Cell cell : r.rawCells()) {
					System.out.println("col��"
							+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
							+ "	value:"
							+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ��������ѯ,����rowkey��ѯΨһһ����¼
	 * 
	 * @param tableName
	 */
	public static void QueryByCondition1(String tableName) {

		HTable table = null;
		try {
			table = new HTable(configuration, tableName);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			Get scan = new Get("112233bbbcccc".getBytes());// ����rowkey��ѯ
			Result r = table.get(scan);
			System.out.println("rowkey:" + new String(r.getRow()));
			for (Cell cell : r.rawCells()) {
				System.out.println(
						"col��" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
								+ "	value:"
								+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * ����������ѯ����ѯ������¼
	 * 
	 * @param tableName
	 */
	public static void QueryByCondition2(String tableName) {

		try {

			HTable table = new HTable(configuration, tableName);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa")); // ����column1��ֵΪaaaʱ���в�ѯ
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("rowkey:" + new String(r.getRow()));
				for (Cell cell : r.rawCells()) {
					System.out.println("col��"
							+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
							+ "	value:"
							+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * ���������ѯ
	 * 
	 * @param tableName
	 */
	public static void QueryByCondition3(String tableName) {

		try {

			HTable table = new HTable(configuration, tableName);

			List<Filter> filters = new ArrayList<Filter>();

			Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes("column1"), null, CompareOp.EQUAL,
					Bytes.toBytes("aaa"));
			filters.add(filter1);

			Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes("column2"), null, CompareOp.EQUAL,
					Bytes.toBytes("bbb"));
			filters.add(filter2);

			Filter filter3 = new SingleColumnValueFilter(Bytes.toBytes("column3"), null, CompareOp.EQUAL,
					Bytes.toBytes("ccc"));
			filters.add(filter3);

			FilterList filterList1 = new FilterList(filters);

			Scan scan = new Scan();
			scan.setFilter(filterList1);
			ResultScanner rs = table.getScanner(scan);
			for (Result r : rs) {
				System.out.println("rowkey:" + new String(r.getRow()));
				for (Cell cell : r.rawCells()) {
					System.out.println("col��"
							+ Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
							+ "	value:"
							+ Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}
			}
			rs.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
