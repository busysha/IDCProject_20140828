package com.xxo.put;

import com.xxo.utils.DateUtils;
import com.xxo.utils.UnZip;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;


/**
 * 合并并写入HDFS
 * Created by xiaoxiaomo on 2016/6/30.
 */
public class FileProcessor implements Runnable {

	@Override
	public void run() {
		// 定义生成文件 指定HDFS文件3
		int counter = 0;
		try {
			String hdfspath = FileWatcher.HDFSPATH
					+ System.getProperty("file.separator");
			// 定义合并后的路径+文件
			String filepath = hdfspath + getDateDir(FileWatcher.INTEVAL)
					+ File.separator + System.nanoTime() + ".txt";
			// 获取HDFS配置信息
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);  //创建hdfs写入对象
			FSDataOutputStream fsout = fs.create(new Path(filepath));
			// 取出队列中的文件 解压 合并 写入hdfs
			int merger_num = Integer.parseInt(FileWatcher.MERGENUM);
			while(counter < merger_num){
				String targetfile = FileWatcher.queue.poll(); //取出该元素并且将该元素从队列中删
				System.out.println("targetfile:"+targetfile);
				if(targetfile != null){
					try {
						UnZip.visitTARGZ(new File(targetfile), fsout); //解压文件变成数据量流
					} catch (Exception e) {
						System.out.println("UnZip error " + targetfile);
						e.printStackTrace();
					}
				}
				counter ++;
			}
			// 关闭资源
			fsout.close();  //关闭流并生成合并后的文件
			System.out.println("put 完成时间 "+ DateUtils.getdateStr());
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// 获取文件夹名
	private String getDateDir(String inteval) {
		String dirname = "";
		if ("hour".equalsIgnoreCase(inteval)) {
			dirname = DateUtils.getHourStr();
		} else if ("day".equalsIgnoreCase(inteval)) {
			dirname = DateUtils.getDayStr();
		} else if ("min".equalsIgnoreCase(inteval)) {
			dirname = DateUtils.get5Min();
		} else {
			dirname = "000000";
		}
		return dirname;
	}

}
