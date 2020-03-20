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
    super(schedulingUniqueID, report.getName()+"("+scheduling.getExternalRepresentation()+")", scheduling.getCron(), Deployment.getBaseTimeZone(), false);
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
        log.info("reportJob " + report.getName() + " : launch report");
        reportService.launchReport(report.getName());
      }
    log.info("reportJob " + report.getName() + " : end execution");
  }

}
