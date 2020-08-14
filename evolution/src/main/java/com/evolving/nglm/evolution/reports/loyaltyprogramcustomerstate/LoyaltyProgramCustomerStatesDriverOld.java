package com.evolving.nglm.evolution.reports.loyaltyprogramcustomerstate;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class LoyaltyProgramCustomerStatesDriverOld extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramCustomerStatesDriverOld.class);
  public static final String SUBSCRIBER_ES_INDEX = "subscriberprofile";

  @Override
  public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing LoyaltyProgramCustomerStates Report with " + report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName(), reportGenerationDate);
    String topic1 = topicPrefix + "-a";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    final String SUBSCRIBER_ES_INDEX = getSubscriberProfileIndex(reportGenerationDate);
    log.debug("data for report : " + topic1 + " " + SUBSCRIBER_ES_INDEX);

    log.debug("PHASE 1 : read ElasticSearch");
    log.trace(topic1 + "," + kafka + "," + zookeeper + "," + elasticSearch + "," + SUBSCRIBER_ES_INDEX);
    LoyaltyProgramCustomerStatesESReader.read(topic1, kafka, zookeeper, elasticSearch, SUBSCRIBER_ES_INDEX, reportGenerationDate, defaultReportPeriodQuantity, defaultReportPeriodUnit);
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } 
    catch (InterruptedException e)
      {
      }

    log.debug("PHASE 3 : write csv file ");
    LoyaltyProgramCustomerStatesCsvWriter.main(new String[] { kafka, topic1, csvFilename });
    ReportUtils.cleanupTopics(topic1);
    log.debug("Finished with LoyaltyProgramCustomerStates Report");
  }

}
