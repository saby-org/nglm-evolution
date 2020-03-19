/*****************************************************************************
 *
 *  ReportScheduler.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ReportService.ReportListener;

/**
 * This class handles the automatic launching of reports, based on the cron-like configuration.
 * NOTE : this is not yet fully implemented.
 *
 */
public class ReportScheduler {
  private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
  private ReportService reportService;
  private JobScheduler reportScheduler;
  private long uniqueID = 0;

  public ReportScheduler() {
    log.trace("Creating ReportService");
    ReportListener reportListener = new ReportListener() {
      @Override public void reportActivated(Report report) {
        log.info("report activated : " + report);
        scheduleReport(report);
      }
      @Override public void reportDeactivated(String guiManagedObjectID) {
        log.info("report deactivated: " + guiManagedObjectID);
      }
    };
    reportService = new ReportService(Deployment.getBrokerServers(), "reportscheduler-reportservice-001", Deployment.getReportTopic(), false, reportListener);
    reportService.start();
    log.trace("ReportService started");
    reportScheduler = new JobScheduler();

    // add special report to display logs
    ScheduledJob specialReportJob = new ScheduledJob(uniqueID++, "special report", "0 * * * *", Deployment.getBaseTimeZone(), true) {
      @Override
      protected void run()
      {
        log.info("===== Special job ");
        List<ScheduledJob> allJobs = reportScheduler.getAllJobs();
        for (ScheduledJob job : allJobs)
          {
            if (job != null)
              {
                log.info("=====   job " + job);
              }
          }
      }
    };
    reportScheduler.schedule(specialReportJob);
    
    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));
  }

  private void run()
  {
//    Date now = SystemTime.getCurrentTime();
//    for (Report report : reportService.getActiveReports(now))
//      {
//        scheduleReport(report);
//      }
    log.info("Starting scheduler");
    reportScheduler.runScheduler();
  }

  private void scheduleReport(Report report)
  {    
    //
    // First deschedule all jobs associated with this report 
    //
    String reportID = report.getReportID();
    for (ScheduledJob job : reportScheduler.getAllJobs())
      {
       if (job != null && job.isProperlyConfigured() && job instanceof ReportJob)
         {
           ReportJob reportJob = (ReportJob) job;
           if (reportID.equals(reportJob.getReportID()))
             {
               log.info("desceduling " + job + " because it runs for " + report.getName());
               reportScheduler.deschedule(job);
             }
         }
      }

    //
    // then schedule or reschedule everything
    //
    for (SchedulingInterval scheduling : report.getEffectiveScheduling())
      {
        log.info("processing "+report.getName()+" with scheduling "+scheduling.getExternalRepresentation()+" cron "+scheduling.getCron());
        ScheduledJob reportJob = new ReportJob(uniqueID++, report, scheduling, reportService);
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
  

  private void unscheduleReport(String reportID)
  {
    GUIManagedObject reportUnchecked = reportService.getStoredReport(reportID);
    log.info("reportUnchecked = "+reportUnchecked);
    if (reportUnchecked != null && reportUnchecked instanceof Report)
      {
        Report report = (Report) reportUnchecked;
        for (Long jobID : report.getJobIDs())
          {
            ScheduledJob job = reportScheduler.findJob(jobID);
            log.info("descheduling "+report.getName()+" for jobID="+jobID+" job="+job);
            if (job != null)
              {
                reportScheduler.deschedule(job);
              }
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
    *  stop threads
    *
    *****************************************/

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
  
  public static void main(String[] args) {
    NGLMRuntime.initialize(true);
    ReportScheduler rs = new ReportScheduler();
    rs.run();
  }

}
