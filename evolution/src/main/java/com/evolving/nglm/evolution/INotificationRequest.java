package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Map;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;

public interface INotificationRequest
{
  public void setCorrelator(String messageId);

  public void setDeliveryStatus(DeliveryStatus deliveryStatus);

  public void setReturnCodeDetails(String returnCodeDetails);

  public void setMessageStatus(MessageStatus status);

  public void setReturnCode(Integer returnCode);

  public DeliveryStatus getDeliveryStatus();

  public MessageStatus getMessageStatus();

  public void setDeliveryDate(Date currentTime);
  
  public String getDeliveryRequestID();

  public String getCorrelator();
  
}
