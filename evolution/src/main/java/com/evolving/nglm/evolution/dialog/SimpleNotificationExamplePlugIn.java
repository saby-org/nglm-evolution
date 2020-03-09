package com.evolving.nglm.evolution.dialog;

import java.util.Date;
import javax.mail.AuthenticationFailedException;
import javax.mail.MessagingException;
import javax.mail.SendFailedException;

import org.apache.log4j.Logger;
import com.evolving.nglm.evolution.MailNotificationManager;
import com.evolving.nglm.evolution.MailNotificationManager.MAILMessageStatus;
import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;
import com.evolving.nglm.evolution.PushNotificationManager;
import com.evolving.nglm.evolution.PushNotificationManager.PushMessageStatus;
import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.lumatagroup.expression.driver.SMTP.NotificationStatusCode;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;
import com.sun.mail.smtp.SMTPMessage;
import com.sun.mail.smtp.SMTPTransport;

/**
 * 
 * SimpleNotificationExamplePlugIn
 *
 */
public class SimpleNotificationExamplePlugIn {
	private static Logger logger = Logger.getLogger(SimpleNotificationExamplePlugIn.class);

	private NotificationManager notificationManager;

	public SimpleNotificationExamplePlugIn(NotificationManager notificationManager) {
		if (logger.isTraceEnabled()) logger.trace("START: SimpleNotificationExamplePlugIn ctor("+notificationManager+")");
		this.notificationManager = notificationManager;
	}

    /**
     * This method has the actual implementation of sending a push notification
     */
	
	public void sendPush(PushNotificationManagerRequest deliveryRequest) throws MessagingException {
	  if(deliveryRequest.getConfirmationExpected()) {
	    deliveryRequest.setDeliveryStatus(DeliveryStatus.Acknowledged);
	    deliveryRequest.setMessageStatus(PushMessageStatus.DELIVERED);
	    deliveryRequest.setReturnCode(PushMessageStatus.DELIVERED.getReturnCode());
	    deliveryRequest.setReturnCodeDetails(PushMessageStatus.DELIVERED.toString());
	    updateDeliveryRequest(deliveryRequest);
	  }else {
	    completeDeliveryRequest(deliveryRequest, deliveryRequest.getDeliveryRequestID(), PushMessageStatus.SENT, DeliveryStatus.Acknowledged, NotificationStatus.SENT.toString());
	  }
	}

	private void updateDeliveryRequest(PushNotificationManagerRequest deliveryRequest){
	  logger.info("SimplePushSender.updateDeliveryRequest(...)");
	  pushNotificationManager.updateDeliveryRequest(deliveryRequest);
	}

	public void completeDeliveryRequest(PushNotificationManagerRequest deliveryRequest, String correlator, PushMessageStatus status, DeliveryStatus deliveryStatus, String returnCodeDetails){
	  deliveryRequest.setCorrelator(correlator);
	  deliveryRequest.setDeliveryStatus(deliveryStatus);
	  deliveryRequest.setMessageStatus(status);
	  deliveryRequest.setReturnCode(status.getReturnCode());
	  deliveryRequest.setReturnCodeDetails(returnCodeDetails);
	  pushNotificationManager.completeDeliveryRequest(deliveryRequest);
	}

}
