/*****************************************************************************
 *
 *  ReportScheduler.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ReportService.ReportListener;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.tenancy.Tenant;

/*****************************************
*
*  class ReportScheduler
*
*****************************************/

public class ReportScheduler {
  
  private static final Logger log = LoggerFactory.getLogger(ReportScheduler.class);
  private ReportService reportService;
  private Map<Integer,JobScheduler> reportScheduler;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ReportScheduler()
  {
    log.trace("Creating ReportScheduler");
    ReportListener reportListener = new ReportListener()
    {
      @Override public void reportActivated(Report report)
      {
        int tenantID = (report != null)? report.getTenantID() : 0;
        log.info("Report activated for tenant " + tenantID + " : " + report);
        scheduleReport(report, tenantID);
      }
      @Override public void reportDeactivated(String guiManagedObjectID)
      {
        log.info("Report deactivated: " + guiManagedObjectID);
      }
    };
    reportService = new ReportService(Deployment.getBrokerServers(), "reportscheduler-reportservice-001", Deployment.getReportTopic(), false, reportListener);
    reportService.start();
    log.trace("ReportService started");
    
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
    log.info("Starting scheduler for all tenants");
    for (Tenant tenant : Deployment.getRealTenants()) {
      int tenantID = tenant.getTenantID();
      log.info("== Starting scheduler for tenant " + tenantID);
      new Thread(reportScheduler.get(tenantID)::runScheduler, "reportScheduler-tenant"+tenantID).start();
    }
    log.info("All schedulers started");
  }
  
  /*****************************************
  *
  *  scheduleReport
  *
  *****************************************/
  
  private void scheduleReport(Report report, int tenantID)
  {
    log.info("Scheduling report " + report.getName() + " for tenant " + tenantID);
    //
    // First deschedule all jobs associated with this report 
    //
    String reportID = report.getReportID();
    for (ScheduledJob job : reportScheduler.get(tenantID).getAllJobs())
      {
       if (job != null && job.isProperlyConfigured() && job instanceof ReportJob && reportID.equals(((ReportJob) job).getReportID()))
         {
           log.info("Descheduling " + job + " for tenant " + tenantID + " because it ran for " + report.getName());
           reportScheduler.get(tenantID).deschedule(job);
         }
      }
    
    //
    // then schedule or reschedule everything
    //
    for (SchedulingInterval scheduling : report.getEffectiveScheduling())
      {
        log.info("Processing "+report.getName()+ " for tenant " + tenantID +" with scheduling "+scheduling.getExternalRepresentation()+" cron "+scheduling.getCron());
        if(scheduling.equals(SchedulingInterval.NONE))
    		continue;
        
        ScheduledJob reportJob = new ReportJob(report, scheduling, reportService, tenantID);
        if(reportJob.isProperlyConfigured())
          {
            log.info("--> scheduling "+report.getName()+ " for tenant " + tenantID +" with jobIDs "+report.getJobIDs());
            reportScheduler.get(tenantID).schedule(reportJob);
          }
        else
          {
            log.info("issue when configuring "+report.getName() + " for tenant " + tenantID);
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
