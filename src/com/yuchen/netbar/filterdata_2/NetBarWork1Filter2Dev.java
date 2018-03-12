package com.yuchen.netbar.filterdata_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NetBarWork1Filter2Dev {
	public static void main(String[] args) throws Exception{
		//1，建立job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2，建立jar包
		job.setJarByClass(NetBarWork1Filter2Dev.class);
		
		//3，建立map/redu的工作类
		job.setMapperClass(NetBarWork1Filter2Map.class);
		job.setReducerClass(NetBarWork1Filter2Red.class);
		
		//4，设置map的输出，类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//9，使用combiner，设置combiner的工作类
		job.setCombinerClass(NetBarWork1Filter2Red.class);
		
		//5，设置最后的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//6，设置文件的输入，输出的输出类型
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outPath = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		//7，提交job
		boolean result = job.waitForCompletion(true);
		
		//8，退出程序
		System.exit(result?0:1);
	}
}
