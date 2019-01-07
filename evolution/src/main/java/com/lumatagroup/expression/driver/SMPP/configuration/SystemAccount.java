package com.lumatagroup.expression.driver.SMPP.configuration;

public class SystemAccount {
	private String systemId;
	private String password;
	private String systemType;
	
	public String getSystemId() {
		return systemId;
	}
	public void setSystemId(final String systemId) {
		this.systemId = systemId;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(final String password) {
		this.password = password;
	}
	public String getSystemType() {
		return systemType;
	}
	public void setSystemType(final String systemType) {
		this.systemType = systemType;
	}
}
