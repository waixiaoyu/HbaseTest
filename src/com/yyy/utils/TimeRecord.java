package com.yyy.utils;

import java.text.SimpleDateFormat;

public class TimeRecord {
	private static  long lStartTime=0;
	private static  long lEndTime=0;
	public static void main(String[] args) throws InterruptedException {
		TimeRecord.start();
		Thread.sleep(3000);
		TimeRecord.stop();
	}
	public static void start()
	{
		lStartTime=System.currentTimeMillis();
		System.out.println("start at: "+new SimpleDateFormat("hh:mm:ss").format(lStartTime));
	}
	public static void stop()
	{
		lEndTime=System.currentTimeMillis();
		System.out.println("end at: "+new SimpleDateFormat("hh:mm:ss").format(lEndTime));
		System.out.println("total is "+(lEndTime-lStartTime)/(1000));
	}
}
