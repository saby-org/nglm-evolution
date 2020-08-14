/****************************************************************************
 *
 *  SubscriberReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.subscriber;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class SubscriberReportDriverOld extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(SubscriberReportDriverOld.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing Subscriber Report with " + report.getName());
    String topic = super.getTopicPrefix(report.getName(), reportGenerationDate) + "-a";
    String esIndexSubscriber = getSubscriberProfileIndex(reportGenerationDate);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    log.debug("PHASE 1 : read ElasticSearch");
    SubscriberReportESReader.read(topic, kafka, zookeeper, elasticSearch, esIndexSubscriber, reportGenerationDate, defaultReportPeriodQuantity, defaultReportPeriodUnit);
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } 
    catch (InterruptedException e)
      {
      }

    log.debug("PHASE 2 : write csv file ");
    SubscriberReportCsvWriter.main(new String[] { kafka, topic, csvFilename });
    ReportUtils.cleanupTopics(topic);
    log.debug("Finished with Subscriber Report");
  }

}