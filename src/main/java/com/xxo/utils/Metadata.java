package com.xxo.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Metadata {	
	Properties prop;
    InputStream inputStream;
	// 返回配置对象
	public Metadata(String filePath){
		 
		prop = new Properties();
		try {
			inputStream = Metadata.class.getResourceAsStream(filePath);
			prop.load(inputStream);
		} catch (Exception e) {
			System.out.println("Exception: init properties error, filePath=" + filePath);
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
	}

	// 获取相应的配置文件信息
	public String getValue(String key) {
		if (prop.containsKey(key)) {
			return prop.getProperty(key);
		} else {
			return "";
		}
	}
}
