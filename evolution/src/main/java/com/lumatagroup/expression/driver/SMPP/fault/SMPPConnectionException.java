package com.lumatagroup.expression.driver.SMPP.fault;


public class SMPPConnectionException extends Exception {

	/**
   * 
   */
  private static final long serialVersionUID = 1L;
  protected String messageId;
	protected String advancedMessage;

	public SMPPConnectionException(Exception exception, final String messageId, String advancedMessage) {
		super(exception);
		this.messageId = messageId;
		this.advancedMessage = advancedMessage;
	}
	
	public SMPPConnectionException(final String messageId) {
		super();
		this.messageId = messageId;
	}

	public SMPPConnectionException(final String messageId, final String advancedMessage) {
		super();
		this.messageId = messageId;
		this.advancedMessage = advancedMessage;
	}

}