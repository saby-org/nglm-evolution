/*****************************************************************************
 *
 *  ReportScheduler.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;
import com.evolving.nglm.evolution.ScheduledJob;

/*****************************************
*
*  class ReportScheduler
*
*****************************************/

public class ReportScheduler {
  
  private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
  private ReportService reportService;
  private JobScheduler reportScheduler;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ReportScheduler()
  {
    log.trace("Creating ReportService");
    ReportListener reportListener = new ReportListener()
    {
      @Override public void reportActivated(Report report)
      {
        log.info("report activated : " + report);
        scheduleReport(report);
      }
      @Override public void reportDeactivated(String guiManagedObjectID)
      {
        log.info("report deactivated: " + guiManagedObjectID);
      }
    };
    reportService = new ReportService(Deployment.getBrokerServers(), "reportscheduler-reportservice-001", Deployment.getReportTopic(), false, reportListener);
    reportService.start();
    log.trace("ReportService started");
    reportScheduler = new JobScheduler("report");
    
    // EVPRO-266 process all existing reports
    for (Report report : reportService.getActiveReports(SystemTime.getCurrentTime(), 0)) // 0 will return for all tenants
      {
        scheduleReport(report);
      }

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/
  
  private void run()
  {
    log.info("Starting scheduler");
    reportScheduler.runScheduler();
  }
  
  /*****************************************
  *
  *  scheduleReport
  *
  *****************************************/
  
  private void scheduleReport(Report report)
  {    
    //
    // First deschedule all jobs associated with this report 
    //
    String reportID = report.getReportID();
    for (ScheduledJob job : reportScheduler.getAllJobs())
      {
       if (job != null && job.isProperlyConfigured() && job instanceof ReportJob && reportID.equals(((ReportJob) job).getReportID()))
         {
           log.info("desceduling " + job + " because it ran for " + report.getName());
           reportScheduler.deschedule(job);
         }
      }

    //
    // then schedule or reschedule everything
    //
    for (SchedulingInterval scheduling : report.getEffectiveScheduling())
      {
        log.info("processing "+report.getName()+" with scheduling "+scheduling.getExternalRepresentation()+" cron "+scheduling.getCron());
        if(scheduling.equals(SchedulingInterval.NONE))
    		continue;
        
        ScheduledJob reportJob = new ReportJob(report, scheduling, reportService);
        if(reportJob.isProperlyConfigured())
          {
            log.info("--> scheduling "+report.getName()+" with jobIDs "+report.getJobIDs());
            reportScheduler.schedule(reportJob);
          }
        else
          {
            log.info("issue when configuring "+report.getName());
          }
      }
  }
  
  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private ReportScheduler reportScheduler;

    //
    //  constructor
    //

    private ShutdownHook(ReportScheduler reportScheduler)
    {
      this.reportScheduler = reportScheduler;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      reportScheduler.shutdownUCGEngine(normalShutdown);
    }
  }

  /****************************************
  *
  *  shutdownUCGEngine
  *
  ****************************************/
  
  private void shutdownUCGEngine(boolean normalShutdown)
  {

    /*****************************************
    *
    *  stop services
    *
    *****************************************/

    reportService.stop(); 

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped ReportScheduler");
  }

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args)
  {
    NGLMRuntime.initialize(true);
    ReportScheduler rs = new ReportScheduler();
    new LoggerInitialization().initLogger();
    rs.run();
  }

}
