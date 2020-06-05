/****************************************************************************
*
*  AutoProvisionSubscriberStreamEvent.java
*
****************************************************************************/

package com.evolving.nglm.core;

public interface AutoProvisionSubscriberStreamEvent extends SubscriberStreamEvent
{
  public void rebindSubscriberID(String subscriberID);
}
