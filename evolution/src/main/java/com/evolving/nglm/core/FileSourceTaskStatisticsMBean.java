/****************************************************************************
*
*  FileSourceTaskStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.core;

public interface FileSourceTaskStatisticsMBean
{
  public String getConnectorName();
  public int getFilesProcessedCount();
  public int getTotalRecordCount();
  public int getErrorRecordCount();
}
