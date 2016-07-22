package com.yyy.testmodel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.yyy.utils.HBaseUtils;

public class CountOrderDetailToHbase {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String tablename = "orderdetail";
		Configuration conf = HBaseUtils.getConfiguration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		createHBaseTable(tablename);
		Job job = Job.getInstance(conf, "order detail");
		job.setJarByClass(CountOrderDetailToHbase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		// 设置输入目录
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.3.201:8020/input/orderdetail/*"));
		System.out.println(job.waitForCompletion(true) ? "完成！" : "非正常退出！");
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text text = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			text.set(s);
			if (text.getLength() == 0) {
				return;
			}
			context.write(text, one);
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			System.out.println("end map...");
		}
	}

	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable> {
		@Override
		protected void setup(Reducer<Text, IntWritable, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			System.out.println("start reduce...");
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable i : values) {
				sum += i.get();
			}
			// Put rowKey
			Put put = new Put(Bytes.toBytes(key.toString()));
			// row,columnFamily:column,value = word,content:count,sum
			put.addColumn(Bytes.toBytes("content"), Bytes.toBytes("count"), Bytes.toBytes(String.valueOf(sum)));
			context.write(NullWritable.get(), put);
		}

		@Override
		protected void cleanup(Reducer<Text, IntWritable, NullWritable, Mutation>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			System.out.println("end reduce...");
		}
	}

	/**
	 * create a table
	 * 
	 * @param tablename
	 * @throws IOException
	 */
	public static void createHBaseTable(String tablename) throws IOException {
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename));
		HColumnDescriptor col = new HColumnDescriptor("content");
		htd.addFamily(col);
		HBaseAdmin admin = (HBaseAdmin) HBaseUtils.getHConnection().getAdmin();
		if (admin.tableExists(tablename)) {
			System.out.println("table exists,trying recreate table!");
			// admin.disableTable(tablename);
			// admin.deleteTable(tablename);
			// admin.createTable(htd);
		} else {
			admin.createTable(htd);
			System.out.println("create new table:" + tablename);
		}
	}
}