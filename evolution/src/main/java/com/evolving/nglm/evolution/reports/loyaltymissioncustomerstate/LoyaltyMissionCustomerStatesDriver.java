/****************************************************************
* 
*  LoyaltyMissionCustomerStatesDriver
*   
*****************************************************************/

package com.evolving.nglm.evolution.reports.loyaltymissioncustomerstate;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;

@ReportTypeDef(reportType = "subscriberprofile")
public class LoyaltyMissionCustomerStatesDriver extends ReportDriver
{
  
  //
  // log
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyMissionCustomerStatesDriver.class);
  
  /****************************************************************
   * 
   * produceReport
   * 
   ***************************************************************/
  
  @Override public void produceReport(Report report, Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    if (log.isDebugEnabled()) log.debug("Processing LoyaltyMissionCustomerStates Report with " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    final String SUBSCRIBER_ES_INDEX = getSubscriberProfileIndex(reportGenerationDate);
    if (log.isDebugEnabled()) log.debug("PHASE 1 : read ElasticSearch");
    LoyaltyMissionCustomerStatesMonoPhase.main(new String[] { elasticSearch, SUBSCRIBER_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    if (log.isDebugEnabled()) log.debug("Finished with LoyaltyMissionCustomerStates Report");
  }

  @Override
  public List<FilterObject> reportFilters()
  {
    return null;
  }

  @Override
  public List<String> reportHeader()
  {
    return null;
  }

}
