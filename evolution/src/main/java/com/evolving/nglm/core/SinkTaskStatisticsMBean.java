/****************************************************************************
*
*  SinkTaskStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.core;

public interface SinkTaskStatisticsMBean
{
  public String getConnectorName();
  public int getBatchCount();
  public int getPutCount();
  public int getRecordCount();
}
