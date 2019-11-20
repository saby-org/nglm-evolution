/*****************************************************************************
*
*  MessageDelivery.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

import java.util.Date;

public interface MessageDelivery
{
  //
  //  standard delivery request accessors
  //

  default public String getMessageDeliverySubscriberID() { return ((DeliveryRequest) this).getSubscriberID(); }
  default public Date getMessageDeliveryEventDate() { return ((DeliveryRequest) this).getCreationDate(); }
  default public String getMessageDeliveryModuleId() { return ((DeliveryRequest) this).getModuleID(); }
  default public String getMessageDeliveryFeatureId() { return ((DeliveryRequest) this).getFeatureID(); }
  default public DeliveryStatus getMessageDeliveryDeliveryStatus() { return ((DeliveryRequest) this).getDeliveryStatus(); }

  //
  //  message delivery accessors
  //

  public int getMessageDeliveryReturnCode();
  public String getMessageDeliveryReturnCodeDetails();
  public String getMessageDeliveryOrigin();
  public String getMessageDeliveryMessageId();
}
