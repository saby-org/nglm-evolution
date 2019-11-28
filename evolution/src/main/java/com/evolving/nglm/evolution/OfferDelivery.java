/*****************************************************************************
*
*  OfferDelivery.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

import java.util.Date;

public interface OfferDelivery
{
  //
  //  standard delivery request accessors
  //

  default public String getOfferDeliverySubscriberID() { return ((DeliveryRequest) this).getSubscriberID(); }
  default public Date getOfferDeliveryEventDate() { return ((DeliveryRequest) this).getCreationDate(); }
  default public String getOfferDeliveryModuleId() { return ((DeliveryRequest) this).getModuleID(); }
  default public String getOfferDeliveryFeatureId() { return ((DeliveryRequest) this).getFeatureID(); }
  default public DeliveryStatus getOfferDeliveryDeliveryStatus() { return ((DeliveryRequest) this).getDeliveryStatus(); }

  //
  //  offer delivery accessors
  //

  public int getOfferDeliveryReturnCode();
  public String getOfferDeliveryReturnCodeDetails();
  public String getOfferDeliveryOrigin();
  public String getOfferDeliveryOfferDisplay();
  public int getOfferDeliveryOfferQty();
  public String getOfferDeliverySalesChannelId();
  public long getOfferDeliveryOfferPrice();
  public String getOfferDeliveryMeanOfPayment();
  public String getOfferDeliveryOfferContent();
  public String getOfferDeliveryVoucherCode();
  public String getOfferDeliveryVoucherPartnerId();
  
}
