package com.xxo.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;



public class DateUtils {

	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static DateFormat daystr = new SimpleDateFormat("yyyyMMdd");
	private static DateFormat hourstr = new SimpleDateFormat("yyyyMMddHH");
	private static DateFormat mstr = new SimpleDateFormat("mm");
	
	
	public static String getHourStr()
	{
	  return 	hourstr.format(new Date());
	}
	public static String  getDayStr()
	{
		return daystr.format(new Date());
	}
	public static String get5Min()
	{
		Date date=new   Date();
		String mistr=new String((Integer.parseInt(mstr.format(date))/5)*5+"");
		if(mistr.length()==1)
		{
			mistr="0"+mistr;
		}
		
	return	getHourStr()+mistr;
	}
	
	public static String getdateStr()
	{
		return df.format(new Date());
	}

	//根据不同粒度获得不同时间
/*	public static String gettargetStr(String inteval)
	{
		String intval="";
		if(FileWatcher.INTEVAL_DAY.equals(inteval))
		{
			intval= getDayStr();
		}else if(FileWatcher
				.INTEVAL_HOUR.equals(inteval))
		{
			intval= getHourStr();
		} else if (FileWatcher.INTEVAL_5MIN.equals(inteval)){
			intval= get5Min();
		}
		return intval;
		 
	}*/
	 
	public static void main(String[] args) {
		System.out.println(get5Min());
	}
	  
}
