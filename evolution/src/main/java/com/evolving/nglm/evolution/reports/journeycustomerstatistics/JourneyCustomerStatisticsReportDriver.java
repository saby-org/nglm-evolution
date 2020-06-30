package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class JourneyCustomerStatisticsReportDriver extends ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {

    log.debug("Processing Journey Customer Statistics Report with " + report + " and " + params);
    String topicPrefix = super.getTopicPrefix(report.getName(), reportGenerationDate);
    String topic1 = topicPrefix + "-a";
    String topic2 = topicPrefix + "-b";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    // We add a random number to make sure each instance of this report starts from
    // scratch
    // If we need to parallelise this phase, remove the random number.
    final String JOURNEY_ES_INDEX = "journeystatistic-";
    final String JOURNEY_METRIC_ES_INDEX = "journeymetric";
    String appIdPrefix = report.getName() + "_" + getTopicPrefixDate(reportGenerationDate);
    log.debug("data for report : " + topic1 + " " + topic2 + " " + JOURNEY_ES_INDEX + " " + JOURNEY_METRIC_ES_INDEX + " " + appIdPrefix);

    JourneyService journeyService = new JourneyService(kafka, "JourneyCustomerStatisticsReport-journeyservice-" + topic1, Deployment.getJourneyTopic(), false);
    journeyService.start();

    log.debug("PHASE 1 : read ElasticSearch");
    log.trace(topic1 + "," + kafka + "," + zookeeper + "," + elasticSearch + "," + JOURNEY_ES_INDEX + "," + JOURNEY_METRIC_ES_INDEX);
    JourneyCustomerStatisticsReportESReader.main(new String[] { topic1, kafka, zookeeper, elasticSearch, JOURNEY_ES_INDEX, JOURNEY_METRIC_ES_INDEX, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, journeyService, reportGenerationDate);
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e)
      {
      }

    log.debug("PHASE 2 : process data");
    JourneyCustomerStatisticsReportProcessor.main(new String[] { topic1, topic2, kafka, zookeeper, appIdPrefix, "1" });
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } 
    catch (InterruptedException e)
      {
      }

    log.debug("PHASE 3 : write csv file ");
    JourneyCustomerStatisticsReportCsvWriter.main(new String[] { kafka, topic2, csvFilename }, journeyService);

    journeyService.stop();
    ReportUtils.cleanupTopics(topic1, topic2, ReportUtils.APPLICATION_ID_PREFIX, appIdPrefix, JourneyCustomerStatisticsReportProcessor.STORENAME);
    log.debug("Finished with Journey Customer Statistics Report");

  }
}