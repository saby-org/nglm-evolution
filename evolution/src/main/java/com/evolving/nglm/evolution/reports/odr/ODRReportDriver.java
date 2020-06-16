package com.evolving.nglm.evolution.reports.odr;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class ODRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing " + report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName(), reportGenerationDate);
    String topic1 = topicPrefix;
    String esIndexOdr = "detailedrecords_offers-";
    String appIdPrefix = report.getName() + "_" + getTopicPrefixDate(reportGenerationDate);

    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();

    log.debug("PHASE 1 : read ElasticSearch");
    ODRReportESReader.main(new String[] { topic1, kafka, zookeeper, elasticSearch, esIndexOdr, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e)
      {
      }

    log.debug("PHASE 2 : write csv file ");
    ODRReportCsvWriter.main(new String[] { kafka, topic1, csvFilename });
    ReportUtils.cleanupTopics(topic1);
    log.debug("Finished with ODR Report");
  }
}
