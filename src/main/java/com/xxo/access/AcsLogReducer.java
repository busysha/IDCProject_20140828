package com.xxo.access;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AcsLogReducer extends Reducer<Text, AcsLogRecord, NullWritable, Text>{
	
	AcsLogRecord acr = new AcsLogRecord();
	private Text text_value = new Text();
	
	Date d ;
	String timedir;
	
	SimpleDateFormat sdf = new SimpleDateFormat("s");
	//多目录输出
	private MultipleOutputs<NullWritable, Text> mout ;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		//初始化多目录输出对象
		mout = new MultipleOutputs<NullWritable, Text>(context);
		d = new Date();
		timedir = sdf.format(d);
	}
	
	
	@Override
	protected void reduce(Text key, Iterable<AcsLogRecord> values,
			Context context) throws IOException,
			InterruptedException {
		//定义在redue方法中
		double duration = 0;
		
		//获取迭代器 用于遍历values
		Iterator<AcsLogRecord> iter = values.iterator();
		while(iter.hasNext()){
			acr = iter.next();
			duration += Double.parseDouble(acr.duration);
		}
		duration = duration/1000;
		
		///拼接输出value
		StringBuffer sb_value = new StringBuffer();
		sb_value.append(key).append("|");
		sb_value.append(String.format("%.2f", duration));
		text_value.set(sb_value.toString());
		
//		context.write(NullWritable.get(), text_value);//问题：1、如果输出目录存在，报错   2、只能输出一次
		
		//多目录输出好处：
		//1、如果输出目录存在，不报错，追加   2、可以输出多次 3、可以定义文件头
		String path = "/user/crxy/accesslogs/"+timedir+"/";
		mout.write(NullWritable.get(), text_value, path);
		
	}
	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		//关闭多目录输出并且生成数据文件
		mout.close();
	}

}
