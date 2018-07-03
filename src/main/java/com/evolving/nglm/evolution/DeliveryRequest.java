/*****************************************
*
*  DeliveryRequest.java
*
*****************************************/  

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;

import org.apache.kafka.connect.data.Schema;

import java.util.Date;

public interface DeliveryRequest extends SubscriberStreamEvent, SubscriberStreamOutput
{
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryRequestID();
  public String getSubscriberID();
  public String getJourneyInstanceID();
  public String getJourneyID();
  public boolean getControl();
  public DeliveryStatus getDeliveryStatus();
  public Date getDeliveryDate();
  public Integer getDeliveryPartition();
  public Date getEventDate();
  public Schema subscriberStreamEventSchema();
  public Object subscriberStreamEventPack(Object value);

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setDeliveryStatus(DeliveryStatus deliveryStatus);
  public void setDeliveryDate(Date deliveryDate);
  public void setDeliveryPartition(Integer deliveryPartition);
}
