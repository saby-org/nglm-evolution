package com.evolving.nglm.evolution.reports.odr;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportEsReader.PERIOD;
import com.evolving.nglm.evolution.reports.bdr.BDRReportProcessor;
import com.evolving.nglm.evolution.reports.journeycustomerstates.JourneyCustomerStatesReportObjects;

import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class ODRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportDriver.class);

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing Subscriber Report with " + report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName());
    String topic1 = topicPrefix + "_a";
    String topic2 = topicPrefix + "_b";
    String esIndexOdr = "detailedrecords_offers-";
    String esIndexCustomer = "subscriberprofile";
    String appIdPrefix = "ODRAppId_" + System.currentTimeMillis();

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
    ReportUtils.cleanupTopics(topic1, topic2, JourneyCustomerStatesReportObjects.APPLICATION_ID_PREFIX, appIdPrefix, ODRReportProcessor.STORENAME);
    log.debug("Finished with ODR Report");
  }
}
