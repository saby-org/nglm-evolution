package com.lumatagroup.expression.driver.SMTP;


public class NotificationStatusMessage {
	private int number = 0;
    private String message = "";

    /**
     * NotificationStatusMessage constructor
     * @param number
     * @param message
     */
    public NotificationStatusMessage(int number, String message) {
        this.number = number;
        this.message = message;
    }

    /**
     * Method to set code
     * @param value
     */
    public void setNumber(int value) {
        number = value;
    }

    /**
     * Method to get number
     * @return number
     */
    public int getNumber() {
        return number;
    }

    /**
     * Method to set the message
     * @param value
     */
    public void setMessage(String value) {
        message = value;
    }

    /**
     * Method to get message
     * @return
     */
    public String getMessage() {
	    return message;
    }

    public String toString() {
        StringBuffer    buffer = new StringBuffer("");

        buffer.append("{");
        buffer.append("number=" + number);
        buffer.append(", message=" + message);
        buffer.append("}");

        return buffer.toString();
    }
}
