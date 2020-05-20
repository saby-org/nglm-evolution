package com.evolving.nglm.evolution.reports.bdr;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class BDRReportDriver extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(BDRReportDriver.class);

  @Override
  public void produceReport(
      Report report,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params) {
        log.debug("Processing Subscriber Report with "+report.getName());
        String topicPrefix = super.getTopicPrefix(report.getName());
        String topic1 = topicPrefix;
        String esIndexBdr = "detailedrecords_bonuses-";
        log.debug("PHASE 1 : read ElasticSearch");
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
        BDRReportESReader.main(new String[]{
            topic1, kafka, zookeeper, elasticSearch, esIndexBdr, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
        });         
        try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}

        log.debug("PHASE 2 : write csv file ");
        BDRReportCsvWriter.main(new String[]{
            kafka, topic1, csvFilename
        });
        ReportUtils.cleanupTopics(topic1);
        log.debug("Finished with BDR Report");

  }

}
