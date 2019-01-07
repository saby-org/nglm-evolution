package com.lumatagroup.expression.driver.dyn;

import com.google.gson.annotations.SerializedName;

public class Xheaders {
	@SerializedName("X-messageId")
	private String XmessageId;

	public String getXmessageId() {
		return XmessageId;
	}

	public void setXmessageId(String xmessageId) {
		XmessageId = xmessageId;
	}
	
}
