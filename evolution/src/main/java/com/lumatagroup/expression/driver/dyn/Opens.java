package com.lumatagroup.expression.driver.dyn;

public class Opens {
	private String ip;
	private String stage;
	private String date;
	private String emailaddress;
	private Xheaders xheaders;
	
	public Xheaders getXheaders() {
		return xheaders;
	}
	public void setXheaders(Xheaders xheaders) {
		this.xheaders = xheaders;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getStage() {
		return stage;
	}
	public void setStage(String stage) {
		this.stage = stage;
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public String getEmailaddress() {
		return emailaddress;
	}
	public void setEmailaddress(String emailaddress) {
		this.emailaddress = emailaddress;
	}
	
}
