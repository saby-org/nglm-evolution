package com.evolving.nglm.evolution.reports.journeycustomerstates;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.HeaderObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

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
		
    log.debug("Processing Journey Customer States Mono Report with "+report+" and "+params);
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
	public List<HeaderObject> reportHeader() {
		// TODO Auto-generated method stub
		return null;
	}
}