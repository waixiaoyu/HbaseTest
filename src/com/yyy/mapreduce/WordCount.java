package com.yyy.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * 
 * ������WordCount explains by Felix
 * 
 * @author Hadoop Dev Group
 */
public class WordCount {
	/**
	 * MapReduceBase��:ʵ����Mapper��Reducer�ӿڵĻ��ࣨ���еķ���ֻ��ʵ�ֽӿڣ���δ���κ����飩 Mapper�ӿڣ�
	 * WritableComparable�ӿڣ�ʵ��WritableComparable��������໥�Ƚϡ����б�����key����Ӧ��ʵ�ִ˽ӿڡ�
	 * Reporter ������ڱ�������Ӧ�õ����н��ȣ�������δʹ�á�
	 * 
	 */
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		/**
		 * LongWritable, IntWritable, Text ���� Hadoop ��ʵ�ֵ����ڷ�װ Java
		 * �������͵��࣬��Щ��ʵ����WritableComparable�ӿڣ�
		 * ���ܹ������л��Ӷ������ڷֲ�ʽ�����н������ݽ���������Խ����Ƿֱ���Ϊlong,int,String �����Ʒ��
		 */
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/**
		 * Mapper�ӿ��е�map������ void map(K1 key, V1 value, OutputCollector<K2,V2>
		 * output, Reporter reporter) ӳ��һ������������k/v�Ե�һ���м��k/v��
		 * ����Բ���Ҫ�����������ͬ�����ͣ�����Կ���ӳ�䵽0����������ԡ�
		 * OutputCollector�ӿڣ��ռ�Mapper��Reducer�����<k,v>�ԡ�
		 * OutputCollector�ӿڵ�collect(k, v)����:����һ��(k,v)�Ե�output
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		/**
		 * JobConf��map/reduce��job�����࣬��hadoop�������map-reduceִ�еĹ���
		 * ���췽����JobConf()��JobConf(Class exampleClass)��JobConf(Configuration
		 * conf)��
		 */
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount"); // ����һ���û������job����
		conf.setOutputKeyClass(Text.class); // Ϊjob�������������Key��
		conf.setOutputValueClass(IntWritable.class); // Ϊjob�������value��
		conf.setMapperClass(Map.class); // Ϊjob����Mapper��
		conf.setCombinerClass(Reduce.class); // Ϊjob����Combiner��
		conf.setReducerClass(Reduce.class); // Ϊjob����Reduce��
		conf.setInputFormat(TextInputFormat.class); // Ϊmap-reduce��������InputFormatʵ����
		conf.setOutputFormat(TextOutputFormat.class); // Ϊmap-reduce��������OutputFormatʵ����
		/**
		 * InputFormat����map-reduce�ж�job�����붨�� setInputPaths():Ϊmap-reduce
		 * job����·��������Ϊ�����б� setInputPath()��Ϊmap-reduce job����·��������Ϊ����б�
		 */
		FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.3.128:9000/input/wordcount/*"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.3.128:9000/input/output"));
		JobClient.runJob(conf); // ����һ��job
	}
}