package com.lumatagroup.expression.driver.SMTP;


import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Bhavishya
 *
 */
public class ResponseCode {
	
		private static Logger logger = LoggerFactory.getLogger(ResponseCode.class);
		private boolean isSuccess = true;
		private List<NotificationStatusMessage> notificationMsgList = new ArrayList<NotificationStatusMessage>(1);
	 	/**
	     * Initializes the response code object.
	     */
	    public ResponseCode() {
	        // Clear the return code
	        reset();
	    }
	    
	    /**
	     * Re-initializes the response code object.
	     */
	    public void reset() {
	        setSuccess(true);
	        notificationMsgList = new ArrayList<NotificationStatusMessage>(1);
	    }
	    /**
	     * Sets the flag indicating success or failure of a method.
	     * @param value     A flag indicating success (true) or failure (false).
	     */
	    public void setSuccess(boolean value) {
		    isSuccess = value;
	    }
	    
	    /**
	     * Adds an notification status to the response code object.
	     * @param number    The notification status number.
	     * @param message   The notification status message.
	     */
	    public void addNotificationStatus(int number, String message) {
	    	addNotificationStatus(number, message, false);
	    }
	    
	    /**
	     * Adds an notification status to the response code object.
	     * @param number    The notification status number.
	     * @param message   The notification status message.
	     * @param isLogged  Flag to indicate if notification status message should be logged.
	     */
	    public void addNotificationStatus(int number, String message, boolean isLogged) {

	        if (isLogged) {
	        	logger.info("Notification number: ["+number+"], message: ["+message+"]");
	        }
	        setSuccess(false);
	        NotificationStatusMessage statusMsg = new NotificationStatusMessage(number, message);
	        notificationMsgList.add(statusMsg);
	    }
	    
	    /**
	     * Returns the list notification status messages.
	     * @return  The list of notification status messages.
	     */
	    public List<NotificationStatusMessage> getNotificationStatusList() {
	        return notificationMsgList;
	    }
	    
	    /**
	     * Returns a single string containing all the notification status messages.
	     * @return  A string of notification status messages.
	     */
	    public String getNotificationStatusMessage() {
	        StringBuffer message = new StringBuffer("");

	        for (int index = 0; index < notificationMsgList.size(); index++) {

	            // If this isn't the first message, add some spacing before the next message.
	            if (index > 0) {
	                message.append("\n\n");
	            }

	            NotificationStatusMessage statusMsg = (NotificationStatusMessage) notificationMsgList.get(index);
	            message.append("Notification status code: "+statusMsg.getNumber() + " 	|	message: " + statusMsg.getMessage());
	        }

	        return message.toString();
	    }

}
