package com.lumatagroup.expression.driver.SMTP.model;



import java.util.Date;


import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.lumatagroup.expression.driver.SMTP.constants.SMTPFeedbackType;
import com.lumatagroup.expression.driver.dyn.DynSMTPTimeWindow;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;


/**
 * author: Bhavisya
 * This class represents a feedback object.
 * 
 */

public class SMTPFeedbackMsg {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5233171474203074033L;
	SMTPFeedbackType feedbackType;
	String description;
	NotificationStatus notificationStatus;
	Date feedbackDate;
	Date notifCreationTimeStamp;
	boolean toResend = true;
	private final AtomicInteger acceptHandletRetryCount;
	private final AtomicInteger deliveryHandleRetryCount;
	private Map<String, String> criteMap;
	private DynSMTPTimeWindow smtpTimeWindow;

	public SMTPFeedbackMsg(String messageId, String messageDriverId, String notes,Date creationTS,  Map<String, String> criteriaMap) {
		this.notifCreationTimeStamp = creationTS;
		this.acceptHandletRetryCount = new AtomicInteger();
		this.deliveryHandleRetryCount = new AtomicInteger();
		this.criteMap = criteriaMap;
	}

	public Map<String, String> getCriteMap() {
		return criteMap;
	}
	public boolean isToResend() {
		return toResend;
	}
	public void setToResend(boolean toResend) {
		this.toResend = toResend;
	}
	
	public Date getFeedbackDate() {
		return feedbackDate;
	}


	public void setFeedbackDate(Date feedbackDate) {
		this.feedbackDate = feedbackDate;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public NotificationStatus getNotificationStatus() {
		return notificationStatus;
	}

	public void setNotificationStatus(NotificationStatus notificationStatus) {
		this.notificationStatus = notificationStatus;
	}

	public SMTPFeedbackType getFeedbackType() {
		return feedbackType;
	}

	public void setFeedbackType(SMTPFeedbackType feedbackType) {
		this.feedbackType = feedbackType;
	}

	public Date getNotifCreationTimeStamp() {
		return notifCreationTimeStamp;
	}
	
	/**
	 * @return the current number of time this specific acceptHandler instance has been processed
	 */
	public int getCurrentAcceptHandletRetryCount() {
		return acceptHandletRetryCount.get();
	}
	
	/**
	 * @return the current number of time this specific acceptHandler instance has been processed and than increment it by 1
	 */
	public int getCurrentAcceptHandletRetryCountAndIncrement() {
		return acceptHandletRetryCount.getAndIncrement();
	}
	
	/**
	 * @return the current number of time this specific acceptHandler instance has been processed
	 */
	public int getCurrentDeliveryHandleRetryCount() {
		return deliveryHandleRetryCount.get();
	}
	
	/**
	 * @return the current number of time this specific acceptHandler instance has been processed and than increment it by 1
	 */
	public int getCurrentDeliveryHandleRetryCountAndIncrement() {
		return deliveryHandleRetryCount.getAndIncrement();
	}
	
	public DynSMTPTimeWindow getSmtpTimeWindow() {
		return smtpTimeWindow;
	}

	public void setSmtpTimeWindow(DynSMTPTimeWindow smtpTimeWindow) {
		this.smtpTimeWindow = smtpTimeWindow;
	}

	public String getMessageId() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getMessageDriverId() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setMessageDriverId(String dialogMgrMessageId) {
		// TODO Auto-generated method stub
		
	}	
}
