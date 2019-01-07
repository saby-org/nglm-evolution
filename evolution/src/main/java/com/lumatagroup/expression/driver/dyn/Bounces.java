package com.lumatagroup.expression.driver.dyn;

import com.google.gson.annotations.SerializedName;

public class Bounces {
	private String bouncetype;
	private String bouncerule;
	private String bounceemail;
	private String bouncemessage;
	private String bouncetime;
	private String notified;
	
	@SerializedName("X-messageId")
	private String XmessageId;
	
		
	public String getXmessageId() {
		return XmessageId;
	}

	public void setXmessageId(String xmessageId) {
		XmessageId = xmessageId;
	}
	
	public String getBouncetype() {
		return bouncetype;
	}
	public void setBouncetype(String bouncetype) {
		this.bouncetype = bouncetype;
	}
	public String getBouncerule() {
		return bouncerule;
	}
	public void setBouncerule(String bouncerule) {
		this.bouncerule = bouncerule;
	}
	public String getBounceemail() {
		return bounceemail;
	}
	public void setBounceemail(String bounceemail) {
		this.bounceemail = bounceemail;
	}
	public String getBouncemessage() {
		return bouncemessage;
	}
	public void setBouncemessage(String bouncemessage) {
		this.bouncemessage = bouncemessage;
	}
	public String getBouncetime() {
		return bouncetime;
	}
	public void setBouncetime(String bouncetime) {
		this.bouncetime = bouncetime;
	}
	public String getNotified() {
		return notified;
	}
	public void setNotified(String notified) {
		this.notified = notified;
	}
	
}
