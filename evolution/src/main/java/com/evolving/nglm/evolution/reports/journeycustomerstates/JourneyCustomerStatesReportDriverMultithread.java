package com.evolving.nglm.evolution.reports.journeycustomerstates;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class JourneyCustomerStatesReportDriverMultithread extends ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportDriverMultithread.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {

    log.debug("Processing Journey Customer States Report Multithread with " + report + " and " + params);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String JOURNEY_ES_INDEX = "journeystatistic-";
    log.debug("PHASE 1 : read ElasticSearch");
    JourneyCustomerStatesReportMultithread.main(new String[] { elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate, tenantID);
    log.debug("Finished with Journey Customer States Report Multithread");

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