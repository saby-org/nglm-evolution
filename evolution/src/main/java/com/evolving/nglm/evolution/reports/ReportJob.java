/*****************************************************************************
 *
 *  ReportJob.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
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
  private static final Logger log = LoggerFactory.getLogger(ReportJob.class);

  public String getReportID() { return (report != null) ? report.getReportID() : null; }
  
  /*****************************************
  *
  *  constructor
  *  
  *****************************************/
  
  public ReportJob(long schedulingUniqueID, Report report, SchedulingInterval scheduling, ReportService reportService)
  {
    super(schedulingUniqueID, report.getName()+"("+scheduling.getExternalRepresentation()+")", scheduling.getCron(), Deployment.getSystemTimeZone(), false); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    this.report = report;
    this.reportService = reportService;
    report.addJobID(schedulingUniqueID);
  }

  @Override
  protected void run()
  {
    log.info("reportJob " + report.getName() + " : start execution");
    if (reportService.isReportRunning(report.getName()))
      {
        log.info("Trying to schedule report "+report.getName()+" but it is already running");
      }
    else
      {
        if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch report");
        log.info("RAJ K launching Report {}", report.getGUIManagedObjectDisplay());
        Thread thread = new Thread( () -> 
        { 
          reportService.launchReport(report, false);
          try { Thread.sleep(30L*1000); } catch (InterruptedException e) { e.printStackTrace(); } // wait for 30s 
          log.info("RAJ K launching pending Report {}", report.getGUIManagedObjectDisplay());
          if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch pending report");
          reportService.launchReport(report, true);
        }
        );
        thread.start();
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
      }
    log.info("reportJob " + report.getName() + " : end execution");
  }

}
