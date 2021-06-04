package com.evolving.nglm.evolution.reports.journeycustomerstates;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;

import java.util.Date;

@ReportTypeDef(reportType = "journeys")
public class JourneyCustomerStatesReportDriver extends ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {

    log.debug("Processing Journey Customer States Report with " + report + " and " + params);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String JOURNEY_ES_INDEX = "journeystatistic-";
    log.debug("PHASE 1 : read ElasticSearch");
    JourneyCustomerStatesReportMonoPhase.main(new String[] { elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with Journey Customer States Report");
  }


  @Override
  public List<FilterObject> reportFilters() {
    return null;
  }

  @Override
  public List<String> reportHeader() {
    // TODO Auto-generated method stub
    return null;
  }
}