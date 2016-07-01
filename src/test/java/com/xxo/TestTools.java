package com.xxo;

import org.junit.Test;

public class TestTools {

	/**
	 * 测试
	 * 电脑核数
	 * 和系统路劲分隔符
	 * 运行结果：4 \
	 */
	@Test
	public void runPara() {

		//电脑核数
		System.out.println(Runtime.getRuntime().availableProcessors());

		//系统路劲分隔符
		System.out.println(System.getProperty("file.separator"));
	}
}
