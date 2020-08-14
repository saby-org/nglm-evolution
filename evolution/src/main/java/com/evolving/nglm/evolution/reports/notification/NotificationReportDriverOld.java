package com.evolving.nglm.evolution.reports.notification;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class NotificationReportDriverOld extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(NotificationReportDriverOld.class);

  @Override
  public void produceReport(
      Report report,
      final Date reportGenerationDate,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params) {
        log.debug("Processing "+report.getName());
        String topicPrefix = super.getTopicPrefix(report.getName(), reportGenerationDate);
        String topic1 = topicPrefix;
        String esIndexNotif = "detailedrecords_messages-";
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
        String appIdPrefix = report.getName() + "_" + getTopicPrefixDate(reportGenerationDate);
        
        log.debug("PHASE 1 : read ElasticSearch");
        NotificationReportESReader.main(new String[]{
            topic1, kafka, zookeeper, elasticSearch, esIndexNotif, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
        }, reportGenerationDate);          
        try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
        
        log.debug("PHASE 2 : write csv file ");
        NotificationReportCsvWriter.main(new String[]{
            kafka, topic1, csvFilename
        });
        ReportUtils.cleanupTopics(topic1);
        log.debug("Finished with BDR Report");
  }
}
