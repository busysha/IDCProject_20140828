package com.xxo.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AcsLogDriver extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3){
			System.out.println("Usage:<inputpath> <outputpath> <reduceNum>");
			ToolRunner.printGenericCommandUsage(System.out);
			System.exit(1);
		}
		//获取集群默认配置
		Configuration conf = getConf();
		Job job = new Job(conf);
		//设置基本参数
		job.setJarByClass(AcsLogDriver.class);
		job.setJobName("collecter");
		job.setMapperClass(AcsLogMapper.class);
		job.setReducerClass(AcsLogReducer.class);
		job.setNumReduceTasks(Integer.parseInt(args[2]));
		//设置输入输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AcsLogRecord.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//设置输入输出目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		return status?0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int run = ToolRunner.run(new AcsLogDriver(), args);
		System.exit(run);
	}

}
