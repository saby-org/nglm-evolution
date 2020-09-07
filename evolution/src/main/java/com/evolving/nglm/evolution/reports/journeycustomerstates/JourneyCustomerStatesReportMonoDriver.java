package com.evolving.nglm.evolution.reports.journeycustomerstates;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.journeycustomerstatistics.JourneyCustomerStatisticsReportProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JourneyCustomerStatesReportMonoDriver extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportMonoDriver.class);
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
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    // We add a random number to make sure each instance of this report starts from scratch
    // If we need to parallelise this phase, remove the random number.
    
    JourneyService journeyService = new JourneyService(kafka, "JourneyCustomerStatesReport-journeyservice-JourneyCustomerStatesReportMonoDriver", Deployment.getJourneyTopic(), false);
    journeyService.start();

    log.debug("PHASE 1 : read ElasticSearch");
		JourneyCustomerStatesReportMonoPhase.main(new String[]{
			elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
		}, journeyService);			
		
		journeyService.stop();
		
	  log.debug("Finished with Journey Customer States Report");
		
	}

	@Override
	public List<FilterObject> reportFilters() {
		// TODO Auto-generated method stub
		return null;
	}
}