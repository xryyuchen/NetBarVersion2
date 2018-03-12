package com.yuchen.netbar.filterdata_2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NetBarWork1Filter2Red extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable count;
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		count = new IntWritable();
	}
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int num = 0 ;
		for(IntWritable v : values){
			num += v.get();
		}
		count.set(num);
		context.write(key, count);
	}
	
}










