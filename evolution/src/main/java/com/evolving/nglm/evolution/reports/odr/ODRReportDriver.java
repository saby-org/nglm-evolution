package com.evolving.nglm.evolution.reports.odr;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class ODRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportDriver.class);

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing Subscriber Report with " + report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName());
    String topic1 = topicPrefix + "-a";
    String topic2 = topicPrefix + "-b";
    String esIndexOdr = "detailedrecords_offers-";
    String esIndexCustomer = "subscriberprofile";
    String appIdPrefix = report.getName() + "_" + getTopicPrefixDate();

    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    
    log.debug("PHASE 1 : read ElasticSearch");
    ODRReportESReader.main(new String[] { topic1, kafka, zookeeper, elasticSearch, esIndexOdr, esIndexCustomer, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit });
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e)
      {
      }

    log.debug("PHASE 2 : process data");
    ODRReportProcessor.main(new String[] { topic1, topic2, kafka, zookeeper, appIdPrefix, "1" });
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e)
      {
      }

    log.debug("PHASE 3 : write csv file ");
    ODRReportCsvWriter.main(new String[] { kafka, topic2, csvFilename });
    ReportUtils.cleanupTopics(topic1, topic2, ReportUtils.APPLICATION_ID_PREFIX, appIdPrefix, ODRReportProcessor.STORENAME);
    log.debug("Finished with ODR Report");
  }
}
