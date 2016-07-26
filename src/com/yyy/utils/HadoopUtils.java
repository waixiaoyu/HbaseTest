package com.yyy.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {
	private static Configuration conf = null;

	/**
	 * 获取全局唯一的Configuration实例
	 * 
	 * @return
	 */
	public static synchronized Configuration getConfiguration() {
		if (conf == null) {
			conf = HBaseUtils.getConfiguration();
		}
		return conf;
	}

	public static void deleteOutputDirectory(Path outputPath) throws IOException {
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			System.out.println("Deleting output path before proceeding.");
			fs.delete(outputPath, true);
		}
	}
}
