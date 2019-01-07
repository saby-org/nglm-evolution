package com.lumatagroup.expression.driver.SMPP.configuration;

public class SMPPConnectionTimeCheck {
	private String timeBetweenReconnection;
	private String timeBetweenConnectionCheck;
	
	public String getTimeBetweenReconnection() {
		return timeBetweenReconnection;
	}
	public void setTimeBetweenReconnection(final String timeBetweenReconnection) {
		this.timeBetweenReconnection = timeBetweenReconnection;
	}
	public String getTimeBetweenConnectionCheck() {
		return timeBetweenConnectionCheck;
	}
	public void setTimeBetweenConnectionCheck(final String timeBetweenConnectionCheck) {
		this.timeBetweenConnectionCheck = timeBetweenConnectionCheck;
	}
}
