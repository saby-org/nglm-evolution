package com.evolving.nglm.evolution.reports.bdr;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

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
        String topic1 = topicPrefix+"_a";
        String topic2 = topicPrefix+"_b";
        String esIndexBdr = "detailedrecords_bonuses-";
        String esIndexCustomer = "subscriberprofile";
        log.debug("PHASE 1 : read ElasticSearch");
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
        String appIdPrefix = "BDRAppId_"+System.currentTimeMillis();
        BDRReportESReader.main(new String[]{
            topic1, kafka, zookeeper, elasticSearch, esIndexBdr, esIndexCustomer, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
        });         
        try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
        
        log.debug("PHASE 2 : process data");
        BDRReportProcessor.main(new String[]{
            topic1, topic2, kafka, zookeeper, appIdPrefix, "1"
        });
        try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}

        log.debug("PHASE 2 : write csv file ");
        BDRReportCsvWriter.main(new String[]{
            kafka, topic2, csvFilename
        });
        log.debug("Finished with BDR Report");

  }

}
