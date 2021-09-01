package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

import java.util.ArrayList;
import java.util.Date;

@ReportTypeDef(reportType = "subscriberprofile")
public class LoyaltyProgramCustomerStatesDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing LoyaltyProgramCustomerStates Report with "+report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    final String SUBSCRIBER_ES_INDEX = getSubscriberProfileIndex(reportGenerationDate);
    log.debug("PHASE 1 : read ElasticSearch");
    LoyaltyProgramCustomerStatesMonoPhase.main(new String[]{elasticSearch, SUBSCRIBER_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit, tenantID+""}, reportGenerationDate);         
    log.debug("Finished with LoyaltyProgramCustomerStates Report");
  }

  @Override
  public List<FilterObject> reportFilters() {
	  return null;
  }

  @Override
  public List<String> reportHeader() {
    List<String> result = LoyaltyProgramCustomerStatesMonoPhase.headerFieldsOrder;
    return result;
  }
}
