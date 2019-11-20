/*****************************************************************************
*
*  BonusDelivery.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

import java.util.Date;

public interface BonusDelivery
{
  //
  //  standard delivery request accessors
  //

  default public String getBonusDeliverySubscriberID() { return ((DeliveryRequest) this).getSubscriberID(); }
  default public Date getBonusDeliveryEventDate() { return ((DeliveryRequest) this).getCreationDate(); }
  default public String getBonusDeliveryModuleId() { return ((DeliveryRequest) this).getModuleID(); }
  default public String getBonusDeliveryFeatureId() { return ((DeliveryRequest) this).getFeatureID(); }
  default public DeliveryStatus getBonusDeliveryDeliveryStatus() { return ((DeliveryRequest) this).getDeliveryStatus(); }

  //
  //  bonus delivery accessors (delivery request)
  //
  
  default public Date getBonusDeliveryDeliverableExpiration() { return ((DeliveryRequest) this).getTimeout(); }

  //
  //  bonus delivery accessors (details)
  //

  public int getBonusDeliveryReturnCode();
  public String getBonusDeliveryReturnCodeDetails();
  public String getBonusDeliveryOrigin();
  public String getBonusDeliveryProviderId();
  public String getBonusDeliveryDeliverableId();
  public int getBonusDeliveryDeliverableQty();
  public String getBonusDeliveryOperation();
}
