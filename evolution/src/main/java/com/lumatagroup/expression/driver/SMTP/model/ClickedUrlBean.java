package com.lumatagroup.expression.driver.SMTP.model;

public class ClickedUrlBean {
	private String messageId;
	private String msisdn;
	private String moduleId;
	private String featureId;
	private String urlClicked;
	private String pollingDateTime;
	
	public String getPollingDateTime() {
		return pollingDateTime;
	}
	public void setPollingDateTime(String pollingDateTime) {
		this.pollingDateTime = pollingDateTime;
	}
	public String getMsisdn() {
		return msisdn;
	}
	public void setMsisdn(String msisdn) {
		this.msisdn = msisdn;
	}
	public String getModuleId() {
		return moduleId;
	}
	public void setModuleId(String moduleId) {
		this.moduleId = moduleId;
	}
	public String getFeatureId() {
		return featureId;
	}
	public void setFeatureId(String featureId) {
		this.featureId = featureId;
	}
	public String getUrlClicked() {
		return urlClicked;
	}
	public void setUrlClicked(String urlClicked) {
		this.urlClicked = urlClicked;
	}
	public String getMessageId() {
		return messageId;
	}
	public void setMessageId(String messageId) {
		this.messageId = messageId;
	}
}
