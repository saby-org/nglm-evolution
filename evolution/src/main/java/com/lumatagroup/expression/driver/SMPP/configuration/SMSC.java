package com.lumatagroup.expression.driver.SMPP.configuration;

import java.util.Properties;

import com.lumatagroup.expression.driver.SMPP.Util.SMPPUtil.SMPP_CONFIGS;

public class SMSC {
	private final String name;
	private final Properties props;
	public SMSC(String name) {
		this.name = name;
		this.props = new Properties();
	}
	public void addProperty(String name, String value) {
		if (this.props.containsKey(name)) {
			String tmp = this.props.getProperty(name);
			if (tmp != null && !tmp.isEmpty()) {
				value = tmp+","+value;
			}
		}
		this.props.put(name, value);
	}
	public String getName() {
		return this.name;
	}
	public String getProperty(SMPP_CONFIGS name) {
		return this.props.getProperty(name.name());
	}
	public String[] getProperties(String name) {
		String tmp = this.props.getProperty(name);
		if (tmp != null) {
			return tmp.split(",");
		} else {
			return null;
		}
	}
	
	public int getSize(){
		return this.props.size();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SMSC [name=");
		builder.append(name);
		builder.append(", props=");
		builder.append(props.toString());
		builder.append("]");
		return builder.toString();
	}
}
