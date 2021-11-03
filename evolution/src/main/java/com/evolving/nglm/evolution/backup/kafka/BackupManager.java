/****************************************************************************
 *
 *  ReportManager.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.backup.kafka;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.ScheduledJob;

/**
 * This class uses Zookeeper to launch the generation of extract.
 * When a node is created in the controlDir, it triggers the generation of a extract.
 * During this generation, an ephemeral node is created in lockDir, to prevent another report (of the same type) to be created.
 */
public class BackupManager {
  private static final Logger log = LoggerFactory.getLogger(BackupManager.class);
  protected static String brokerServers;
  private static BackupManagerStatistics backupManagerStatistics;
  private static SimpleDateFormat sdf;
  private JobScheduler jobScheduler;
  
  static
  {
    try
    {
      sdf = new SimpleDateFormat(Deployment.getBackupManagerDateFormat());   // TODO EVPRO-99
      sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getDefault().getTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct
    }
    catch (IllegalArgumentException e)
    {
      log.error("Config error : date format " + Deployment.getBackupManagerDateFormat() + " is invalid, using default"
          + e.getLocalizedMessage(), e);
      sdf = new SimpleDateFormat(); // Default format, might not be valid in a filename, sigh...   // TODO EVPRO-99
    }
  }


  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public BackupManager()
  {
    log.trace("Creating BackupManager");
    jobScheduler = new JobScheduler("backup");
    
    /*
    // create a ReportScheduler per tenant
    reportScheduler = new HashMap<>();
    for (Tenant tenant : Deployment.getRealTenants()) {
      int tenantID = tenant.getTenantID();
      log.info("Creating scheduler for tenant " + tenantID);
      reportScheduler.put(tenantID, new JobScheduler("report-"+tenantID));
    }
    
    // EVPRO-266 process all existing reports
    for (Tenant tenant : Deployment.getRealTenants()) {
      int tenantID = tenant.getTenantID();
      for (Report report : reportService.getActiveReports(SystemTime.getCurrentTime(), tenantID)) {
        scheduleReport(report, tenantID);
      }
    }
    */
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  private void run()
  {
    String fileExtension = Deployment.getBackupManagerFileExtension();
    String fileSuffix = sdf.format(SystemTime.getCurrentTime());
    String outputPath = Deployment.getBackupManagerOutputPath();
    for (String topic : Deployment.getBackupManagerTopics()) {
      ScheduledJob backupJob = new BackupJob(topic, Deployment.getBackupManagerCronEntry(), outputPath, sdf, fileExtension);
      if (backupJob.isProperlyConfigured()) {
        log.info("--> scheduling "+topic);
        jobScheduler.schedule(backupJob);
      } else {
        log.info("issue when configuring "+topic);
      }
      new Thread(jobScheduler::runScheduler).start();
    }
  }
  
  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    BackupManager bm = new BackupManager();
    new LoggerInitialization().initLogger();
    brokerServers = args[0];
    backupManagerStatistics = new BackupManagerStatistics("backupmanager");
    bm.run();
  }
}
