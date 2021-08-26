/****************************************************************
* 
*  LoyaltyChallengeCustomerStatesDriver
*   
****************************************************************/

package com.evolving.nglm.evolution.reports.loyaltychallengecustomerstate;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;
import com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate.LoyaltyProgramCustomerStatesMonoPhase;

@ReportTypeDef(reportType = "subscriberprofile")
public class LoyaltyChallengeCustomerStatesDriver extends ReportDriver
{
  //
  // log
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyChallengeCustomerStatesDriver.class);

  /****************************************************************
   * 
   * produceReport
   * 
   ***************************************************************/

  @Override public void produceReport(Report report, Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    if (log.isDebugEnabled()) log.debug("Processing LoyaltyChallengeCustomerStates Report with " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    final String SUBSCRIBER_ES_INDEX = getSubscriberProfileIndex(reportGenerationDate);
    if (log.isDebugEnabled()) log.debug("PHASE 1 : read ElasticSearch");
    LoyaltyChallengeCustomerStatesMonoPhase.main(new String[] { elasticSearch, SUBSCRIBER_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    if (log.isDebugEnabled()) log.debug("Finished with LoyaltyChallengeCustomerStates Report");
  }

  @Override
  public List<FilterObject> reportFilters()
  {
    return null;
  }

  @Override
  public List<String> reportHeader()
  {
    List<String> result = LoyaltyChallengeCustomerStatesMonoPhase.headerFieldsOrder;
    return result;
  }

}
