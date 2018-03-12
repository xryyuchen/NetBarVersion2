package com.yuchen.netbar.filterdata_2;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class NetBarFilter2Bean{
	
	private String idCard;
	private String name;
	private long startTime;
	private static SimpleDateFormat sdf;
	
	static{
		sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	}
	private static final long TWOHOUR = 7200000;
	public boolean withinTwoHour(NetBarFilter2Bean o){
		if(o.startTime - startTime <= TWOHOUR)
			return true;
		return false;
	}
//	@Override
//	public void write(DataOutput out) throws IOException {
//		out.writeUTF(idCard);
//		out.writeUTF(name);
//		out.writeLong(startTime);
//	}
//
//	@Override
//	public void readFields(DataInput in) throws IOException {
//		idCard = in.readUTF();
//		name = in.readUTF();
//		startTime = in.readLong();
//	}
	
	public String getIdCard() {
		return idCard;
	}

	public void setIdCard(String idCard) {
		this.idCard = idCard;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		try {
			this.startTime = sdf.parse(startTime).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	

}
