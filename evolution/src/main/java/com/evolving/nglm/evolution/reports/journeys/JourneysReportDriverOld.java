package com.evolving.nglm.evolution.reports.journeys;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JourneysReportDriverOld extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(JourneysReportDriverOld.class);
  public static final String JOURNEY_ES_INDEX = "journeystatistic*";

  @Override
  public void produceReport(
      Report report,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params) {
        log.debug("Processing Journeys Report with "+report.getName());
        String topicPrefix = super.getTopicPrefix(report.getName());
        
        String topic1 = topicPrefix+"-a";
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
        log.debug("PHASE 1 : read ElasticSearch");
        log.trace(topic1+","+kafka+","+zookeeper+","+elasticSearch+","+JOURNEY_ES_INDEX);
        JourneysReportESReader.main(new String[]{
            topic1, kafka, zookeeper, elasticSearch, JOURNEY_ES_INDEX, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
        });         
        try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
        
        log.debug("PHASE 2 : write csv file ");
        JourneysReportCsvWriter.main(new String[]{
                kafka, topic1, csvFilename
        });
        ReportUtils.cleanupTopics(topic1);
        log.debug("Finished with Journeys Report");
    }

@Override
public List<FilterObject> reportFilters() {
	// TODO Auto-generated method stub
	return null;
}

}
