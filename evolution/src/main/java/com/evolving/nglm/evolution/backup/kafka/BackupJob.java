/*****************************************************************************
 *
 *  ReportJob.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.backup.kafka;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ScheduledJob;

public class BackupJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String topic;
  private String path;
  private SimpleDateFormat sdf;
  private String fileExtension;
  private static final Logger log = LoggerFactory.getLogger(BackupJob.class);

  
  /*****************************************
  *
  *  constructor
   * @param fileExtension 
   * @param sdf 
  *  
  *****************************************/
  
  public BackupJob(String topic, String cron, String path, SimpleDateFormat sdf, String fileExtension)
  {
    super(topic, cron, Deployment.getDefault().getTimeZone(), false);
    this.topic = topic;
    this.path = path;
    this.sdf = sdf;
    this.fileExtension = fileExtension;
  }

  @Override
  protected void run()
  {
    String fileSuffix = sdf.format(SystemTime.getCurrentTime());
    String filename = path + File.separator + topic + "_" + fileSuffix + "." + fileExtension;
    log.info("reportJob " + topic + " : start execution on file " + filename);
    try
      {
        BackupTopic.backupTopic(topic, filename);
      }
    catch (IOException e)
      {
        log.error("Exception while doing backup of topic " + topic + " to file " + filename);
      }
    log.info("reportJob " + topic + " : end execution");
  }

}
