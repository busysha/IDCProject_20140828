package com.xxo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigUtils {
	
	// 返回配置对象
	public static Properties getConfig(String filePath) {
		InputStream inputStream = null;
		Properties prop = new Properties();
		try {
			inputStream = ConfigUtils.class.getResourceAsStream(filePath);
			prop.load(inputStream);
		} catch (Exception e) {
			System.out.println("init properties error: " + filePath);
			e.printStackTrace();
		}finally {
			if (inputStream != null) {
				try {
					inputStream.close();
				} catch (IOException e) {
					System.out.println("can't close the inputstream!");
					e.printStackTrace();
				}
			}
		}
		return prop;
	}
	
	public static void main(String[] args) {
		Properties prop = ConfigUtils.getConfig("/config/redis_meta_data.properties");
		System.out.println(prop.getProperty("REDIS.HOST"));
	}
}
