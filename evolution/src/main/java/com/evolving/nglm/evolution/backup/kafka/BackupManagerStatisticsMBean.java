/****************************************************************************
*
*  ReportManagerStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution.backup.kafka;

public interface BackupManagerStatisticsMBean
{
  public int getBackupCount();
  public int getFailureCount();
}
