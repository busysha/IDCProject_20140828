package com.xxo.execip;

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

/**
 * Created by xiaoxiaomo on 2016/6/30.
 */
public class IPDriver extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        if(args.length != 2){
            System.out.println("Usage:<inputpath> <outputpath> args :" + args.length );
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(1);
        }
        //1. 获取集群默认配置
        Configuration conf = getConf();
        Job job = new Job(conf);

        //设置基本参数
        job.setJarByClass(IPDriver.class);
        job.setJobName(IPDriver.class.getSimpleName());

        job.setMapperClass(IPMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置输入输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        return status?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new IPDriver(), args);
        System.exit(run);
    }
}
