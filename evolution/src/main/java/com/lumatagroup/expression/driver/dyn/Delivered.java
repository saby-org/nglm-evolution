package com.lumatagroup.expression.driver.dyn;


public class Delivered {
	private String senttime;
	private String emailaddress;
	private String userid;
	private int mssenttime;
	private Xheaders xheaders;
	
	public Xheaders getXheaders() {
		return xheaders;
	}
	public void setXheaders(Xheaders xheaders) {
		this.xheaders = xheaders;
	}
	
	public String getSenttime() {
		return senttime;
	}
	public void setSenttime(String senttime) {
		this.senttime = senttime;
	}
	public String getEmailaddress() {
		return emailaddress;
	}
	public void setEmailaddress(String emailaddress) {
		this.emailaddress = emailaddress;
	}
	public String getUserid() {
		return userid;
	}
	public void setUserid(String userid) {
		this.userid = userid;
	}
	public int getMssenttime() {
		return mssenttime;
	}
	public void setMssenttime(int mssenttime) {
		this.mssenttime = mssenttime;
	}
	
}
