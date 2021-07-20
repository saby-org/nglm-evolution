package com.evolving.nglm.evolution.reports.journeycustomerstatistics;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.journeycustomerstates.JourneyCustomerStatesReportMultithread;

@ReportTypeDef(reportType = "journeys")
public class JourneyCustomerStatisticsReportDriverMultithread extends ReportDriver {

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatisticsReportDriverMultithread.class);
  public static final String JOURNEY_ES_INDEX = "journeystatistic-";

  @Override
  public void produceReport(
        Report report,
        final Date reportGenerationDate,
        String zookeeper,
        String kafka,
        String elasticSearch,
        String csvFilename,
        String[] params,
        int tenantID) {
      
      log.debug("Processing Journey Customer Statistics Report Multithread with "+report+" and "+params);
      
      // Might not be supported for this report (not used later)
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
      
      log.debug("data for report : "+JOURNEY_ES_INDEX);

      JourneyCustomerStatisticsReportMultithread.main(new String[]{
          elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
      }, reportGenerationDate, tenantID);         
      try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
      
      log.debug("Finished with Journey Customer Statistics Report Multithread");
      
  }
  

  @Override
  public List<FilterObject> reportFilters() {
    return null;
  }

  @Override
  public List<String> reportHeader() {
    List<String> result = JourneyCustomerStatisticsReportMultithread.headerFieldsOrder;
    return result;
  }
  
}