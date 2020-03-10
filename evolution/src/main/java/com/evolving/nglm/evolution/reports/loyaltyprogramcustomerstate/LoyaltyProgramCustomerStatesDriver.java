package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class LoyaltyProgramCustomerStatesDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesDriver.class);
  public static final String SUBSCRIBER_ES_INDEX = "subscriberprofile";

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing LoyaltyProgramCustomerStates Report with "+report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName());
    
    String topic1 = topicPrefix+"_a";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String appIdPrefix = "JourneyAppId_"+System.currentTimeMillis();
    log.debug("data for report : "
            +topic1+" "+SUBSCRIBER_ES_INDEX+" "+appIdPrefix);

    log.debug("PHASE 1 : read ElasticSearch");
    log.trace(topic1+","+kafka+","+zookeeper+","+elasticSearch+","+SUBSCRIBER_ES_INDEX);
    LoyaltyProgramCustomerStatesESReader.main(new String[]{
        topic1, kafka, zookeeper, elasticSearch, SUBSCRIBER_ES_INDEX, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
    });         
    try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
    
    log.debug("PHASE 3 : write csv file ");
    LoyaltyProgramCustomerStatesCsvWriter.main(new String[]{
            kafka, topic1, csvFilename
    });
    log.debug("Finished with LoyaltyProgramCustomerStates Report");
  }

}
