package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.HeaderObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class LoyaltyProgramCustomerStatesDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesDriver.class);
  public static final String SUBSCRIBER_ES_INDEX = "subscriberprofile";

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing LoyaltyProgramCustomerStates Report with "+report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();

    log.debug("PHASE 1 : read ElasticSearch");
    LoyaltyProgramCustomerStatesMonoPhase.main(new String[]{
        elasticSearch, SUBSCRIBER_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
    });         
    log.debug("Finished with LoyaltyProgramCustomerStates Report");
  }

@Override
public List<HeaderObject> reportHeader() {
	// TODO Auto-generated method stub
	return null;
}

}
