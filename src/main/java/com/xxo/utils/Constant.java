package com.xxo.utils;

public interface Constant {
	
	 // 系统常用常量
	public static final String JDBC_CONFIG_PATH = "/jdbc.properties";
	public static final String REDIS_CONFIG_PATH = "/config/redis_meta_data.properties";
	public static final String EMPTY = "";
	
	// redis同步时用到的系统常量
	public static final String ALIAS = ".ORACEL_ALIAS";
	public static final String COLUMNS = ".COLUMNS";
	public static final String KEY_VALUE = ".KEY_VALUE";
	public static final String ORDERS = ".ORDERS";
	public static final String WHERE_CAUSE = ".WHERE_CAUSE";
}
