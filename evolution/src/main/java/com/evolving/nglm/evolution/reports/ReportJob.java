/*****************************************************************************
 *
 *  ReportJob.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagerLoyaltyReporting;
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

  private String reportID;
  private ReportService reportService;
  private String scheduling;
  private String reportName;
  private int tenantID;
  private static final Logger log = LoggerFactory.getLogger(ReportJob.class);

  public String getReportID() { return reportID; }
  
  /*****************************************
  *
  *  constructor
  *  
  *****************************************/
  
  public ReportJob(Report report, SchedulingInterval scheduling, ReportService reportService, int tenantID)
  {
    // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    super(report.getName()+"_t"+tenantID+"("+scheduling.getExternalRepresentation()+")", scheduling.getCron(), Deployment.getDefault().getTimeZone(), false);
    this.reportID = report.getReportID();
    this.reportService = reportService;
    this.scheduling = scheduling.getExternalRepresentation();
    this.reportName = report.getName();
    this.tenantID = tenantID;
  }

  @Override
  protected void run()
  {
    log.info("reportJob " + reportName + " : start execution in tenant " + tenantID);
    // Need to get report each time to pick up latest changes done in extractScheduling
    Report report = reportService.getActiveReport(reportID, SystemTime.getCurrentTime());
    if (report == null) {
      log.info("Trying to schedule reportID " + reportID + " (was " + reportName + ") in tenant " + tenantID + " but it is not active anymore");
    } else {
      if (!report.getJobIDs().contains(getSchedulingID())) {
        report.addJobID(getSchedulingID());
      }
      if (reportService.isReportRunning(report.getName(), tenantID))
        {
          log.info("Trying to schedule report "+report.getName()+" in tenant " + tenantID + " but it is already running");
        }
      else
        {
          if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch report in tenant " + tenantID);

          //
          // light job threads to launch (to launch scheduled jobs asynchronously) EVPRO-1005
          //

          Thread thread = new Thread( () -> 
          {
            reportService.launchReport(report, tenantID, false);
            try { Thread.sleep(180L*1000); } catch (InterruptedException e) { e.printStackTrace(); } // wait for 180s (3mins) - enough to launch todays report - then launch the other dates report
            if (log.isInfoEnabled()) log.info("reportJob " + report.getName() + " : launch pending report in tenant " + tenantID);

            // wait for report to finish

            while (reportService.isReportRunning(report.getName(), tenantID)) {
              log.info("Report " + report.getName() + " for tenant " + tenantID + " still running, waiting...");
              try { Thread.sleep(60L*1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
            log.info("Report " + report.getName() + " for tenant " + tenantID + " has finished, now check extracts");

            // EVPRO-1121 Scheduling of reports extractions
            /*
             "extractScheduling" : [{
                "scheduling" : ["daily"],
                "filtering" : {
                  "criteria" : null,
                  "percentage" : 10,
                  "topRows" : null,
                  "header" : ["subscriberID", "age", "amountUsageLastMonth"],
                  "columnRemoval" : null
                }
              },
              {
                "scheduling" : ["monthly"],
                "filtering" : {
                  "criteria" : null,
                  "percentage" : null,
                  "topRows" : null,
                  "header" : null,
                  "columnRemoval" : ["msisdn", "contractID"]
                }
              }  
            ]
             */

            JSONArray extractSchedulingJSONArray = JSONUtilities.decodeJSONArray(report.getJSONRepresentation(), Report.EXTRACT_SCHEDULING, false);
            if (extractSchedulingJSONArray != null) { 
              for (int i=0; i<extractSchedulingJSONArray.size(); i++) {
                JSONObject extractSchedulingJSON = (JSONObject) extractSchedulingJSONArray.get(i);
                log.info("Found extractScheduling for tenant " + tenantID + " : " + extractSchedulingJSON);
                JSONArray schedulingJSONArray = JSONUtilities.decodeJSONArray(extractSchedulingJSON, "scheduling", false);
                if (schedulingJSONArray != null) { 
                  for (int j=0; j<schedulingJSONArray.size(); j++) {
                    String schedulingStr = (String) schedulingJSONArray.get(i);
                    if (schedulingStr.equalsIgnoreCase(scheduling)) { // this is the right "extractSscheduling" section
                      JSONObject filteringJSON = (JSONObject) extractSchedulingJSON.get("filtering");
                      filteringJSON.put("id", report.getReportID());
                      filteringJSON.put("tenantID", tenantID);
                      log.info("Found filtering in tenant " + tenantID + " : " + filteringJSON);
                      JSONObject jsonResponse = new JSONObject();
                      try {
                        String filename = GUIManagerLoyaltyReporting.processDownloadReportInternal("0", filteringJSON, jsonResponse, null, reportService);
                        log.info("===> Extract for report " + report.getName() + " for tenant " + tenantID + " available in file " + filename);
                      } catch (IOException e) {
                        log.error("Exception when filtering in tenant " + tenantID + " with " + filteringJSON + " : " + jsonResponse + " " + e.getLocalizedMessage());
                      }
                      break;
                    }
                  }
                }
              }
            }
            
            // Launch pending reports
            reportService.launchReport(report, tenantID, true);
          }
              );
          thread.setName(report.getName() + " for tenant " + tenantID + " launcher");
          thread.start();
        }
    }
    log.info("reportJob " + report.getName() + " : end execution in tenant " + tenantID);
  }

}
