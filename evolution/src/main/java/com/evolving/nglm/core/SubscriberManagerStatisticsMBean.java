/****************************************************************************
*
*  SubscriberManagerStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.core;

public interface SubscriberManagerStatisticsMBean
{
  public int getAutoProvisionCount();
  public int getAssignSubscriberIDCount();
  public int getUpdateSubscriberIDCount();
}
