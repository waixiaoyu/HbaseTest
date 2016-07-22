package com.yyy.testmodel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.yyy.utils.HadoopUtils;
import com.yyy.utils.TimeRecord;

public class SortOutput {

	private static final String FAMILY = "content";
	private static final String QUALIFIER = "count";

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		TimeRecord.start();
		String sourceTable = "orderdetailr";
		Path outputPath = new Path("hdfs://192.168.3.201:8020/output/");
		Configuration conf = HadoopUtils.getConfiguration();
		Job job = Job.getInstance(conf, "test detail");
		job.setJarByClass(SortOutput.class);

		Scan scan = new Scan();
		scan.setCaching(500); // 1 is the default in Scan, which will be bad
		// for
		// MapReduce jobs
		scan.setCacheBlocks(false); // don't set to true for MR jobs
		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob(sourceTable, // input table
				scan, // Scan instance to control CF and attribute selection
				Map.class, // mapper class
				DoubleWritable.class, // mapper output key
				Text.class, // mapper output value
				job);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setNumReduceTasks(1);

		HadoopUtils.deleteOutputDirectory(outputPath);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		TimeRecord.stop();

	}

	public static class Map extends TableMapper<DoubleWritable, Text> {
		private DoubleWritable dw = new DoubleWritable();
		private Text text = new Text();

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, DoubleWritable, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String strValue = new String(value.getValue(FAMILY.getBytes(), QUALIFIER.getBytes()));

			dw.set(Double.valueOf(strValue));
			text.set(new String(key.get()));

			context.write(dw, text);
		}
	}

	public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		@Override
		protected void reduce(DoubleWritable key, Iterable<Text> texts,
				Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			for (Text text : texts) {
				context.write(text, key);
			}
		}
	}
}