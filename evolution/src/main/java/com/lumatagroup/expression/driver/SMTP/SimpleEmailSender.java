package com.lumatagroup.expression.driver.SMTP;

import java.util.Date;
import javax.mail.AuthenticationFailedException;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;

import org.apache.log4j.Logger;
import com.evolving.nglm.evolution.MailNotificationManager;
import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;
import com.sun.mail.smtp.SMTPMessage;
import com.sun.mail.smtp.SMTPTransport;

/**
 * 
 * @author Bhavishya
 *
 */
public class SimpleEmailSender {
	private static Logger logger = Logger.getLogger(SimpleEmailSender.class);
	private SMTPConnection smtpConn;
	private String fromEmail;
	private String replyTo;

	private String forceSubject;
	private MailNotificationManager mailNotificationManager;

	public SimpleEmailSender(MailNotificationManager mailNotificationManager, String name, SMTPConnection conn, String replyTo, String frmEmail, String forceSubject) {
		if (logger.isTraceEnabled()) logger.trace("START: SimpleEmailSender ctor("+mailNotificationManager+" "+name+" "+conn+" "+replyTo+" "+frmEmail);
		this.smtpConn = conn;
		this.replyTo = replyTo;
		this.fromEmail = frmEmail;
		this.forceSubject = forceSubject;
		this.mailNotificationManager = mailNotificationManager;
		if (conn == null) {
			throw new NullPointerException("Missing argument for SimpleMailSender constructor");
		}
	}

	/**
	 * This method will take the required argument from DialogMangerMessage and
	 * pass them to to the send () because we have to send a
	 * SMTP message to the SMTP Server.
	 * 
	 * @param expandedMsg ExpandedMessageContent
	 * @param deliveryRequest SMTPFeedbackMsg
	 * @throws MessagingException 
	 */
	public void sendEmail(MailNotificationManagerRequest deliveryRequest) throws MessagingException {
		if (logger.isTraceEnabled()) logger.trace("START: SimpleEmailSender.sendEmail("+deliveryRequest+")");
		String emailText = deliveryRequest.getTextBody();
		String toEmail = deliveryRequest.getDestination();
//		String dmMessageId = deliveryRequest.getDeliveryRequestID();
		if (logger.isTraceEnabled()) logger.trace("SimpleEmailSender.sendEmail this.fromEmail="+this.fromEmail+" getSender=" +deliveryRequest.getSource());
		// EFOGC-5467 : we give priority to this.fromEmail because it also includes the "Friendly From" field.
		//              dmMsg.getSender().getSender() only contains the "real" email address.
		// String frmEmail = ((dmMsg.getSender().getSender() == null || dmMsg.getSender().getSender().isEmpty()) ? this.fromEmail : dmMsg.getSender().getSender());
		String frmEmail = this.fromEmail;
		if (frmEmail == null)
			frmEmail = deliveryRequest.getSource();
		String subject = (null == forceSubject ? "" : forceSubject);
		
		send(frmEmail, subject, emailText, replyTo, toEmail, deliveryRequest);

	}

	/**
	 * This method has the actual implementation of sending email to the SMTP
	 * @param frmEmail
	 * @param emailSubj
	 * @param emailText
	 * @param replyTo
	 * @param toEmail
	 * @param deliveryRequest
	 * @param dmMMsgId
	 * @param dmMsg
	 * @return SMTPFeedbackMsg
	 * @throws MessagingException
	 */
	private void send(String frmEmail, String emailSubj, String emailText, String replyTo, String toEmail, 
								  MailNotificationManagerRequest mailNotificationRequest) throws MessagingException, SendFailedException {
	    if (logger.isTraceEnabled()) logger.trace("START: SimpleEmailSender.send("+frmEmail+", "+emailSubj+", "+emailText+", "+replyTo+", "+toEmail+", "+mailNotificationRequest.getDeliveryRequestID()+", ..., ...) execution.");
	    else if (logger.isDebugEnabled()) logger.debug("START: SimpleEmailSender.send() execution.");
		if (smtpConn != null) {
			//START: Bhavishya Comment: Just for tracking the email by appending the message id along with the subject
			//END: Bhavishya Comment: Just for tracking the email by appending the message id along with the subject
			SMTPTransport trans = smtpConn.getTransportObject();
			SMTPMessage smtpMsg = smtpConn.createSMTPMessage(frmEmail, emailSubj, emailText, toEmail, replyTo);
			
			if(smtpMsg != null && trans!=null && smtpMsg.getHeader("From")!=null){
				try {
					smtpMsg.addHeader("X-messageId", mailNotificationRequest.getDeliveryRequestID());
					//logger.info("is SMTP transport connected? "+trans.isConnected());
					if (logger.isDebugEnabled()) logger.debug("Inside SimpleEmailSender.send() last active time: "+smtpConn.getLastActiveTime()+" : "+new Date(smtpConn.getLastActiveTime()));
					long timeout = (smtpConn.getLastActiveTime()) + Long.parseLong(smtpConn.getConnectionTimoutVal().trim());
					if (logger.isDebugEnabled()) logger.debug("Timeout value : "+timeout+" "+new Date(timeout));
					boolean isTimeout = smtpConn.isTimeOut(timeout);
					if (!isTimeout) {
						trans.sendMessage(smtpMsg, smtpMsg.getAllRecipients());
						//delivered
                        if(mailNotificationRequest.getConfirmationExpected()) {
                          mailNotificationRequest.setDeliveryStatus(DeliveryStatus.Acknowledged);
                          mailNotificationRequest.setMessageStatus(MAILMessageStatus.DELIVERED);
                          mailNotificationRequest.setReturnCode(MAILMessageStatus.DELIVERED.getReturnCode());
                          updateDeliveryRequest(mailNotificationRequest);
                      }else {
                          completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.SENT, DeliveryStatus.Acknowledged, NotificationStatusCode.fromReturnCode(NotificationStatusCode.EMAIL_SENT.getReturnCode()).toString());
                        }
						smtpConn.setLastActiveTime(System.currentTimeMillis());
						logger.info("Email sent successfully of X-messageId: "+mailNotificationRequest.getDeliveryRequestID()+" to the recepient address "+toEmail );
					} else {
						//Reconnect in case trans.isConnected is false.
						try{
						    if (logger.isDebugEnabled()) logger.debug("Reconnecting SMTP Transport...");
						    if (logger.isDebugEnabled()) logger.debug("Authentication flag: "+smtpConn.isAuthFlag());
							trans = smtpConn.reconnect();
							smtpConn.setTransportObject(trans);
							//delivered
							trans.sendMessage(smtpMsg, smtpMsg.getAllRecipients());	
	                         if(mailNotificationRequest.getConfirmationExpected()) {
                               mailNotificationRequest.setDeliveryStatus(DeliveryStatus.Acknowledged);
                               mailNotificationRequest.setMessageStatus(MAILMessageStatus.DELIVERED);
                               mailNotificationRequest.setReturnCode(MAILMessageStatus.DELIVERED.getReturnCode());
                               updateDeliveryRequest(mailNotificationRequest);
                           }else {
                             completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.SENT, DeliveryStatus.Acknowledged, NotificationStatusCode.fromReturnCode(NotificationStatusCode.EMAIL_SENT.getReturnCode()).toString());
                           }
							smtpConn.setLastActiveTime(System.currentTimeMillis());
							logger.info("Email sent successfully of X-messageId: "+mailNotificationRequest.getDeliveryRequestID()+" to the recepient address "+toEmail);
						}
						catch(AuthenticationFailedException authException){
							logger.error("AuthenticationFailedException occured in SimpleEmailSender.sendEmail() while reconnect : "+ authException);
							completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatus.ERROR.toString());
							
						}
					}
				}
				catch(AuthenticationFailedException authException){
					logger.error("AuthenticationFailedException occured in SimpleEmailSender.sendEmail(): "+ authException);
					completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.AUTHENTICATION_FAILED.getReturnCode()).toString());
				}
				catch(SendFailedException sendFailedEx){
					logger.error("SendFailedException occured in SimpleEmailSender.sendEmail() while reconnect : "+ sendFailedEx);
					completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.AUTHENTICATION_FAILED.getReturnCode()).toString());
				}
				catch(MessagingException mEx){
					boolean isConnectionError = false;
					logger.error("MessagingException occured in SimpleEmailSender.sendEmail() while reconnect : "+ mEx);
					if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.CAN_NOT_OPEN_CONNECTION.getReturnCode()){
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.CAN_NOT_OPEN_CONNECTION.getReturnCode()).toString());
						isConnectionError = true;
					}else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.CONNECTION_DROPPED.getReturnCode()){
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.CONNECTION_DROPPED.getReturnCode()).toString());
						isConnectionError = true;
					}else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.CONNECTION_REFUSED.getReturnCode()){
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.CONNECTION_REFUSED.getReturnCode()).toString());
						isConnectionError = true;
					}else{
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.UNKNOWN.getReturnCode()).toString());
						isConnectionError = true;
					}

					if(isConnectionError){
						smtpConn.isSNMPConnectionStopAlarmSend = true;
//						this.smtpDriver.sendConnectionStatusTrap(false,null);
					}
					throw new MessagingException(mEx.getMessage());
				}
				catch(Exception ex){
					logger.error("Exception occured in SimpleEmailSender.sendEmail() while reconnect : "+ ex);
					if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.SERVER_NOT_AVAILABLE.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.SERVER_NOT_AVAILABLE.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.MAILBOX_FULL.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.QUEUE_FULL, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.MAILBOX_FULL.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.DISK_FULL.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.DISK_FULL.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.OUTGOING_MESSAGE_TIMEOUT.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.OUTGOING_MESSAGE_TIMEOUT.getReturnCode()).toString());
					
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.ROUTING_ERROR.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.ROUTING_ERROR.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.REQUESTED_ACTION_NOT_TAKEN.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.REQUESTED_ACTION_NOT_TAKEN.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.PAGE_UNAVAILABLE.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.PAGE_UNAVAILABLE.getReturnCode()).toString());
					
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.BAD_EMAIL_ADDRESS.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.BAD_EMAIL_ADDRESS.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.BAD_EMAIL_ADDRESS_1.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.BAD_EMAIL_ADDRESS_1.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.DNS_ERROR.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.DNS_ERROR.getReturnCode()).toString());
					
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.ADDRESS_TYPE_INCORRECT.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.ADDRESS_TYPE_INCORRECT.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.TOO_BIG_MESSAGE.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.INVALID, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.TOO_BIG_MESSAGE.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.AUTHENTICATION_IS_REQUIRED.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.AUTHENTICATION_IS_REQUIRED.getReturnCode()).toString());
					
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.RECIPIENT_ADDRESS_REJECTED.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.RECIPIENT_ADDRESS_REJECTED.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.BLACK_LIST.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.BLACK_LIST.getReturnCode()).toString());
					else if(trans!=null && trans.getLastReturnCode() == NotificationStatusCode.ADDRESS_FAILED.getReturnCode())
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.ADDRESS_FAILED.getReturnCode()).toString());
					else 
					  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNKNOWN, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.UNKNOWN.getReturnCode()).toString());
				}
			}else if (smtpMsg!=null && smtpMsg.getHeader("From")==null){
			  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.UNDELIVERABLE, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.UNKNOWN.getReturnCode()).toString());
				logger.error("Invalid Sender address found in SimpleEmailSender.send().");
			}else{
			  completeDeliveryRequest(mailNotificationRequest, mailNotificationRequest.getDeliveryRequestID(), MAILMessageStatus.ERROR, DeliveryStatus.Failed, NotificationStatusCode.fromReturnCode(NotificationStatusCode.CAN_NOT_OPEN_CONNECTION.getReturnCode()).toString());
				if (logger.isDebugEnabled()) logger.debug("Feedback Call for Accept Handler for messageId: "+mailNotificationRequest.getCorrelator());
				if (logger.isDebugEnabled()) logger.debug("SMTP message and SMTP transport found null in SimpleEmailSender.send().");
				throw new MessagingException("Can not open connection");
			}
			
		} else {
		  if (logger.isDebugEnabled()) logger.debug("SMTP connection problem in SimpleEmailSender.sendEmail()");
		}
	}

	private void updateDeliveryRequest(MailNotificationManagerRequest deliveryRequest){
	  logger.info("SimpleSMSSender.updateDeliveryRequest(message sent");
	  mailNotificationManager.updateDeliveryRequest(deliveryRequest);
	}

	public void completeDeliveryRequest(MailNotificationManagerRequest mailNotif, String messageId, MAILMessageStatus status, DeliveryStatus deliveryStatus, String returnCodeDetails){
	  mailNotif.setCorrelator(messageId);
	  mailNotif.setDeliveryStatus(deliveryStatus);
	  mailNotif.setMessageStatus(status);
	  mailNotif.setReturnCode(status.getReturnCode());
	  mailNotif.setReturnCodeDetails(returnCodeDetails);
	  mailNotificationManager.completeDeliveryRequest(mailNotif);
	}

}
