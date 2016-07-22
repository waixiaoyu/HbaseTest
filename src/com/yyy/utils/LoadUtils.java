package com.yyy.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class LoadUtils {

  public static final String WORDS_PATH = "/dict/web2";
  public static final String NAMES_PATH = "/dict/propernames";

  public static List<String> readResource(String path) throws IOException {
    List<String> lines = new ArrayList<String>();
    String line;
    FileReader rr=new FileReader(new File(path));
    BufferedReader reader = new BufferedReader(rr);
    while ((line = reader.readLine()) != null) {
      lines.add(line);
    }
    reader.close();
    rr.close();
    return lines;
  }

  public static int randInt(int max) {
    return (int)Math.floor(Math.random() * max);
  }

  public static String randNth(List<String> words) {
    int val = randInt(words.size());
    return words.get(val);
  }
}
