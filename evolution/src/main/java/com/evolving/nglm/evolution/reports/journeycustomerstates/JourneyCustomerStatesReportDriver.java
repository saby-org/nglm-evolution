package com.evolving.nglm.evolution.reports.journeycustomerstates;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class JourneyCustomerStatesReportDriver extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportDriver.class);
  public static final String JOURNEY_ES_INDEX = "journeystatistic-";
	  
	@Override
	public void produceReport(
	      Report report,
	      String zookeeper,
	      String kafka,
	      String elasticSearch,
	      String csvFilename,
	      String[] params) {
		
    log.debug("Processing Journey Customer States Report with "+report+" and "+params);
    String topicPrefix = super.getTopicPrefix(report.getName());
    String topic1 = topicPrefix;
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    // We add a random number to make sure each instance of this report starts from scratch
    // If we need to parallelise this phase, remove the random number.
    log.debug("PHASE 1 : read ElasticSearch");
		JourneyCustomerStatesReportESReader.main(new String[]{
			topic1, kafka, zookeeper, elasticSearch, JOURNEY_ES_INDEX, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
		});			
		try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
		
		log.debug("PHASE 2 : write csv file ");
		JourneyCustomerStatesReportCsvWriter.main(new String[]{
				kafka, topic1, csvFilename
		});
    ReportUtils.cleanupTopics(topic1);
	  log.debug("Finished with Journey Customer States Report");
		
	}
}