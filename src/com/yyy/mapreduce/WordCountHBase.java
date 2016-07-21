package com.yyy.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.yyy.utils.HBaseUtils;


public class WordCountHBase {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		String tablename = "wordcount";
		Configuration conf = HBaseUtils.getConfiguration();
		conf.set(TableOutputFormat.OUTPUT_TABLE, tablename);
		createHBaseTable(tablename);
		Job job = new Job(conf, "WordCount table");
		job.setJarByClass(WordCountHBase.class);
		job.setNumReduceTasks(3);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TableOutputFormat.class);
		// 设置输入目录
		FileInputFormat.addInputPath(job, new Path("hdfs://192.168.3.201:8020/input/wordcount/*"));
		System.out.println(job.waitForCompletion(true) ? "完成！" : "非正常退出！");
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text text = new Text();
		
		public static enum Counters {
			WORDS_NUMBER
		};
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer st = new StringTokenizer(s);
			while (st.hasMoreTokens()) {
				text.set(st.nextToken());
				text = remainCharacters(text);
				if (text.getLength() == 0) {
					return;
				}
				context.getCounter(Counters.WORDS_NUMBER).increment(1);
				context.write(text, one);
			}
		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			System.out.println(context.getCounter(Counters.WORDS_NUMBER).getValue());
		}
		private static Text remainCharacters(Text text) {
			String str = text.toString();
			if (str.length() == 0) {
				return text;
			}
			StringBuffer result = new StringBuffer();
			for (int i = 0; i < str.length(); i++) {
				char a = str.charAt(i);
				if (a >= 65 && a <= 90 || a >= 97 && a <= 122 || a >= 48 && a <= 57) {
					result.append(a);
				}
			}
			text.set(result.toString());
			return text;
		}
	}

	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable> {

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
			System.out.println("create new table:" + tablename);
			admin.createTable(htd);
		}
	}
}