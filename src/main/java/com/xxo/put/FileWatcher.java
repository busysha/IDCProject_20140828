package com.xxo.put;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.contentobjects.jnotify.JNotify;
import net.contentobjects.jnotify.JNotifyListener;

/**
 * 监控类
 * 监听某个目录，并上传到HDFS
 * Created by xiaoxiaomo on 2016/6/30.
 */
public class FileWatcher extends Thread{

	public static String LOCALPATH ;	//本地目录
	public static String HDFSPATH  ;	//HDFS路径
	public static String MERGENUM  ;	//合并数量
	public static String INTEVAL   = "none";
	
	//定义个队列
	public static Queue<String> queue = new ConcurrentLinkedQueue<String>();
	
	//定义个文件计数器
	int flieCounter = 0;
	
	//创建线程  防止线程过多导致机器宕机
	ExecutorService pool;
	
	public FileWatcher(){
		
		//Executors.newFixedThreadPool用于定义线程
		pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() - 2);
		
	}
	
	public static void main(String[] args) {

		//验证参数
		if(args.length != 4){
			System.out.println("Usage: <localpath> <HDFSPath> <mergeNum> <inteval(day、hour、min、none)> ");
			System.exit(1);
		}
		//获取参数
		LOCALPATH = args[0];
		HDFSPATH  = args[1];
		MERGENUM  = args[2];
		INTEVAL   = args[3];
		
		//初始化参数
		FileWatcher fileWatcher = new FileWatcher();
		
		//转换为守护进  -- 般需要长时间运行的进程需要设置
		fileWatcher.setDaemon(true);
		
		//开始监控
		fileWatcher.start();
		
	}


	/*
	 * 重写start方法
	 */
	public synchronized void start() {
		//调用Jnotify监控方法
		try {
			this.startWatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 监控方法
	 */
	private void startWatch() throws Exception {
		//指定监听事件  创建文件
		int mask = JNotify.FILE_CREATED;
		
		//添加监控方法
		//参数：监控的目录
		//参数二：监控事件
		//参数三：是否监听子目  true代表监听子目 false
		//参数:监听
		JNotify.addWatch(LOCALPATH, mask, true, new listener());
		
		//监听频率
		while(true){
			//每秒钟扫描一下监控目
			Thread.sleep(1000);
		}
	}
	
	/**
	 * JNotifyListener实现
	 *
	 */
	class listener implements JNotifyListener{

		//监听创建事件的方
		@Override
		public synchronized void fileCreated(int wd, String rootPath, String name) {
			println("fileCreated:" + rootPath + "/" + name);

			//校验是否.ok文件生成
			if( name.contains(".ok") ){
				println(".ok文件生成" + name);
				queue.add(rootPath + "/" + name.replace(".ok", ".tar.gz"));
				flieCounter ++;  //紧跟在add之后

				if(flieCounter == Integer.parseInt(MERGENUM)){ //如果添加的文件数达到用户指定合并数时  始执
					//执行合并写入操作
					pool.execute(new FileProcessor());
					flieCounter = 0;
				}
			}
		}


		//监听删除事件的方
		@Override
		public void fileDeleted(int wd, String rootPath, String name) {
			println("fileCreated:" + rootPath + "/" + name);
		}

		//监听修改事件的方
		@Override
		public void fileModified(int wd, String rootPath, String name) {
			println("fileCreated:" + rootPath + "/" + name);
		}

		//监听重命名事件的方法
		@Override
		public void fileRenamed(int wd, String rootPath, String oldName,String newName) {
			println("fileCreated:" + rootPath + "/" + oldName +"->"+ newName);
		}
		void println(String msg){
			System.out.println(msg);
		}
		
	}
}
