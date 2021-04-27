/*****************************************************************************
 *
 *  ReportJob.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.Report.SchedulingInterval;
import com.evolving.nglm.evolution.ReportService;
import com.evolving.nglm.evolution.ScheduledJob;

public class ReportJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Report report;
  private ReportService reportService;
  private int tenantID;
  private static final Logger log = LoggerFactory.getLogger(ReportJob.class);

  public String getReportID() { return (report != null) ? report.getReportID() : null; }
  
  /*****************************************
  *
  *  constructor
  *  
  *****************************************/
  
  public ReportJob(Report report, SchedulingInterval scheduling, ReportService reportService, int tenantID)
  {
    super(report.getName()+"("+scheduling.getExternalRepresentation()+")", scheduling.getCron(), Deployment.getDefault().getTimeZone(), false); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    this.report = report;
    this.reportService = reportService;
    this.tenantID = tenantID;
    report.addJobID(getSchedulingID());
  }

  @Override
  protected void run()
  {
    log.info("reportJob " + report.getName() + " : start execution");
    if (reportService.isReportRunning(report.getName(), tenantID))
      {
        log.info("Trying to schedule report "+report.getName()+" but it is already running");
      }
    else
      {
        if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch report");
        
        //
        // light job threads to launch (to launch scheduled jobs asynchronously) EVPRO-1005
        //
        
        Thread thread = new Thread( () -> 
        { 
          reportService.launchReport(report, tenantID, false);
          try { Thread.sleep(300L*1000); } catch (InterruptedException e) { e.printStackTrace(); } // wait for 300s (5mins) - enough to launch todays report - then launch the other dates report
          if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch pending report");
          reportService.launchReport(report, tenantID, true);
        }
        );
        thread.setName(report.getName() + " launcher");
        thread.start();
      }
    log.info("reportJob " + report.getName() + " : end execution");
  }

}
