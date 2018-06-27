/****************************************************************************
*
*  GUIServiceStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

public interface GUIServiceStatisticsMBean
{
  public String getServiceName();
  public int getActiveCount();
  public int getObjectCount();
  public int getPutCount();
  public int getRemoveCount();
  public String getLastObjectIDPut();
  public String getLastObjectIDRemoved();
  public Date lastPutTime();
  public Date lastRemoveTime();
}
