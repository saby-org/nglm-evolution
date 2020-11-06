package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class JourneyCustomerStatisticsReportDriver extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportDriver.class);
  public static final String JOURNEY_ES_INDEX = "journeystatistic-";

  @Override
  public void produceReport(
        Report report,
        final Date reportGenerationDate,
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

      
      JourneyCustomerStatisticsReportMonoPhase.main(new String[]{
          elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
      }, reportGenerationDate);         
      try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
      
      log.debug("Finished with Journey Customer Statistics Report");
      
  }
}