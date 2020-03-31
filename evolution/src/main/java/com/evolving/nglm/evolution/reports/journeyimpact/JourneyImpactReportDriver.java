package com.evolving.nglm.evolution.reports.journeyimpact;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class JourneyImpactReportDriver extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyImpactReportDriver.class);
  public static final String JOURNEY_METRIC_ES_INDEX = "journeymetric";
  public static final String JOURNEY_STATS_ES_INDEX = "journeystatistic-";

  @Override
  public void produceReport(
        Report report,
        String zookeeper,
        String kafka,
        String elasticSearch,
        String csvFilename,
        String[] params) {
      
      log.debug("Processing Journey Impact Report with "+report+" and "+params);
      String topicPrefix = super.getTopicPrefix(report.getName());
      String topic1 = topicPrefix+"_a";
      String topic2 = topicPrefix+"_b";
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
      // We add a random number to make sure each instance of this report starts from scratch
      // If we need to parallelise this phase, remove the random number.
      String appIdPrefix = "JourneyAppId_"+System.currentTimeMillis();
      log.debug("data for report : "
          +topic1+" "+topic2+" "+JOURNEY_STATS_ES_INDEX+" "+JOURNEY_METRIC_ES_INDEX+" "+appIdPrefix);

      log.debug("PHASE 1 : read ElasticSearch");
      log.trace(topic1+","+kafka+","+zookeeper+","+elasticSearch+","+JOURNEY_STATS_ES_INDEX+","+JOURNEY_METRIC_ES_INDEX);
      JourneyImpactReportESReader.main(new String[]{
          topic1, kafka, zookeeper, elasticSearch, JOURNEY_STATS_ES_INDEX, JOURNEY_METRIC_ES_INDEX, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
      });         
      try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
      
      log.debug("PHASE 2 : process data");
      JourneyImpactReportProcessor.main(new String[]{
          topic1, topic2, kafka, zookeeper, appIdPrefix, "1"
      });
      try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
      
      log.debug("PHASE 3 : write csv file ");
      JourneyImpactReportCsvWriter.main(new String[]{
          kafka, topic2, csvFilename
      });
      log.debug("Finished with Journey Impact Report");
      
  }
}