package com.yyy.base;

import java.io.File;
import java.io.FileWriter;
import java.util.List;

import com.yyy.utils.LoadUtils;

public class CreateNamesText {

	public static final String OUTPUT_PATH="C:/Users/Administrator/Desktop/name.txt";
	
	public static final String usage = "loadusers count\n" + "  help - print this message and exit.\n"
			+ "  count - add count random TwitBase users.\n";

	private static String randName(List<String> names) {
		String name = LoadUtils.randNth(names) + " ";
		name += LoadUtils.randNth(names);
		return name;
	}


	public static void main(String[] args) throws Exception {
		args = new String[1];
		args[0] = "10000000";
		if (args.length == 0 || "help".equals(args[0])) {
			System.out.println(usage);
			System.exit(0);
		}

		int count = Integer.parseInt(args[0]);
		List<String> names = LoadUtils.readResource(LoadUtils.NAMES_PATH);

		FileWriter out = new FileWriter(createFile(new File(OUTPUT_PATH)));

		for (int i = 0; i < count; i++) {
			String name = randName(names);
			System.out.println(name);
			out.write(name);
			out.write("\n");
		}
		out.close();
	}

	public static File createFile(File fileName) throws Exception {
		try {
			if (!fileName.exists()) {
				fileName.createNewFile();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return fileName;
	}
}
