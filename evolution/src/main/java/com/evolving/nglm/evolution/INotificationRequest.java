package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;
import com.evolving.nglm.evolution.NotificationManager.MessageStatus;

public interface INotificationRequest
{

  void setCorrelator(String messageId);

  void setDeliveryStatus(DeliveryStatus deliveryStatus);

  void setReturnCodeDetails(String returnCodeDetails);

  void setMessageStatus(MessageStatus status);

  void setReturnCode(Integer returnCode);

  DeliveryStatus getDeliveryStatus();

}
