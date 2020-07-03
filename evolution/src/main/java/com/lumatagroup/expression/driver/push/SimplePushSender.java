package com.lumatagroup.expression.driver.push;

import javax.mail.MessagingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.evolution.INotificationRequest;
import com.lumatagroup.expression.driver.dyn.NotificationStatus;

/**
 * 
 * SimplePushSender
 *
 */
public class SimplePushSender {
	private static Logger logger = LoggerFactory.getLogger(SimplePushSender.class);

	private DeliveryManagerForNotifications pushNotificationManager;

	public SimplePushSender(DeliveryManagerForNotifications pushNotificationManager) {
		if (logger.isTraceEnabled()) logger.trace("START: SimplePushSender ctor("+pushNotificationManager+")");
		this.pushNotificationManager = pushNotificationManager;
	}

    /**
     * This method has the actual implementation of sending a push notification
     */
	
	public void sendPush(INotificationRequest deliveryRequest, boolean confirmationExpected) throws MessagingException {
	  if(confirmationExpected) {
	    deliveryRequest.setDeliveryStatus(DeliveryStatus.Acknowledged);
	    deliveryRequest.setMessageStatus(MessageStatus.DELIVERED);
	    deliveryRequest.setReturnCode(MessageStatus.DELIVERED.getReturnCode());
	    deliveryRequest.setReturnCodeDetails(MessageStatus.DELIVERED.toString());
	    updateDeliveryRequest(deliveryRequest);
	  }else {
	    completeDeliveryRequest(deliveryRequest, deliveryRequest.getDeliveryRequestID(), MessageStatus.SENT, DeliveryStatus.Acknowledged, NotificationStatus.SENT.toString());
	  }
	}

	private void updateDeliveryRequest(INotificationRequest deliveryRequest){
	  logger.info("SimplePushSender.updateDeliveryRequest(...)");
	  pushNotificationManager.updateDeliveryRequest(deliveryRequest);
	}

	public void completeDeliveryRequest(INotificationRequest deliveryRequest, String correlator, MessageStatus status, DeliveryStatus deliveryStatus, String returnCodeDetails){
	  deliveryRequest.setCorrelator(correlator);
	  deliveryRequest.setDeliveryStatus(deliveryStatus);
	  deliveryRequest.setMessageStatus(status);
	  deliveryRequest.setReturnCode(status.getReturnCode());
	  deliveryRequest.setReturnCodeDetails(returnCodeDetails);
	  pushNotificationManager.completeDeliveryRequest(deliveryRequest);
	}

}
