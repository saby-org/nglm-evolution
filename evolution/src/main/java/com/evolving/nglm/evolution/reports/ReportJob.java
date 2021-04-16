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
  private static final Logger log = LoggerFactory.getLogger(ReportJob.class);

  public String getReportID() { return (report != null) ? report.getReportID() : null; }
  
  /*****************************************
  *
  *  constructor
  *  
  *****************************************/
  
  public ReportJob(Report report, SchedulingInterval scheduling, ReportService reportService)
  {
    super(report.getName()+"("+scheduling.getExternalRepresentation()+")", scheduling.getCron(), Deployment.getDefault().getTimeZone(), false); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    this.report = report;
    this.reportService = reportService;
    report.addJobID(getSchedulingID());
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
        reportService.launchReport(report);
      }
    log.info("reportJob " + report.getName() + " : end execution");
  }

}
