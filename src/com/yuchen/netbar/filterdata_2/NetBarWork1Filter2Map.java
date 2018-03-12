package com.yuchen.netbar.filterdata_2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NetBarWork1Filter2Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Context context;
	private List<NetBarFilter2Bean> lists ;
	private Text outKey ; //map输出key值
	private String areaBar;          // 标记map输出时采用的areabar
	private String currentAreaBar;   //标记当前输入的areabar
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		this.context = context;
		lists = new ArrayList<>();
		outKey = new Text();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//area	bar	idcard				name	starttime
		//HB021	11	610824200011193173	解蠹眠	2016-02-26 04:05:14

		String[] lines = value.toString().split("\t");
		NetBarFilter2Bean bean = new NetBarFilter2Bean();
		currentAreaBar = lines[0] + "\t" + lines[1];
		bean.setIdCard(lines[2]);
		bean.setName(lines[3]);
		bean.setStartTime(lines[4]);
		analyseAreaBar(bean);
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		while(lists.size() > 1){
			contextAreaBean();
			lists.remove(0);
		}
		lists.clear();
	}
	
	public void analyseAreaBar(NetBarFilter2Bean bean){
		//第一次输入
		if(null == areaBar){
			areaBar = currentAreaBar;
			lists.add(bean);
		}
		else {
			//同一个areaBar
			if(areaBar.equals(currentAreaBar)) {
				listsAddAreaBean(bean);
			}else{ //接收到下一个areaBar，处理list缓存的数据，
				while(lists.size() > 1){
					contextAreaBean();
					lists.remove(0);
				}
				lists.clear();
				areaBar = currentAreaBar;		//areaBar 重新赋值
				lists.add(bean);
			}
		}
	}
	public void listsAddAreaBean(NetBarFilter2Bean bean){
		while(lists.size() > 0 && !lists.get(0).withinTwoHour(bean)){
			if(lists.size() > 1)
				contextAreaBean();
			lists.remove(0);
		}
		lists.add(bean);
	}
	public void contextAreaBean(){
		NetBarFilter2Bean vfirst = lists.get(0);
		for(int i = 1 ; i < lists.size() ;i++){
			NetBarFilter2Bean value = lists.get(i);
			
			if(vfirst.getIdCard().equals(value.getIdCard())) continue;
			
			if(vfirst.getIdCard().compareTo(value.getIdCard()) > 0){
				outKey.set(areaBar + "\t" + vfirst.getIdCard() +"\t" + vfirst.getName() + "\t" + 
						value.getIdCard() +"\t" + value.getName() + "\t");
			}else{
				outKey.set(areaBar + "\t" + value.getIdCard() +"\t" + value.getName() + "\t" + 
						vfirst.getIdCard() +"\t" + vfirst.getName() + "\t");
			}
			try {
				context.write(outKey,new IntWritable(1));
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
