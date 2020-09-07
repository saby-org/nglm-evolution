package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class JourneyCustomerStatisticsReportMonoDriver extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportMonoDriver.class);
  public static final String JOURNEY_ES_INDEX = "journeystatistic-";

  @Override
  public void produceReport(
        Report report,
        String zookeeper,
        String kafka,
        String elasticSearch,
        String csvFilename,
        String[] params) {
      
      log.debug("Processing Journey Customer Statistics Report with "+report+" and "+params);
      
      // Might not be supported for this report (not used later)
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
      
      log.debug("data for report : "+JOURNEY_ES_INDEX);

      JourneyService journeyService = new JourneyService(kafka, "JourneyCustomerStatisticsReport-journeyservice-JourneyCustomerStatisticsReportMonoDriver", Deployment.getJourneyTopic(), false);
      journeyService.start();
      
      JourneyCustomerStatisticsReportMonoPhase.main(new String[]{
          elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
      }, journeyService);         
      try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
      
      journeyService.stop();
      log.debug("Finished with Journey Customer Statistics Report");
      
  }

@Override
public List<FilterObject> reportFilters() {
	// TODO Auto-generated method stub
	return null;
}
}