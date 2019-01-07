package com.lumatagroup.expression.driver.dyn;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DynSMTPTimeWindow implements Serializable{
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3843211826677085210L;

	private Date startTime ;
	
	private Date endTime;
	private String timeZoneStr;
	private String dateFormatStr;
	SimpleDateFormat f ;
	public DynSMTPTimeWindow(String timeZone, String dateFormat){
		this.timeZoneStr = timeZone;
		this.dateFormatStr = dateFormat;
		this.f = new SimpleDateFormat(dateFormatStr);
	}	
	
	public Date getStartTime() {
		return this.startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return this.endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	
	public String getFormatedStartTime() {
		f.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
		return f.format(this.startTime);
	}
	
	public String getFormatedEndTime() {
		f.setTimeZone(TimeZone.getTimeZone(timeZoneStr));
		return f.format(this.endTime);
	}

}
