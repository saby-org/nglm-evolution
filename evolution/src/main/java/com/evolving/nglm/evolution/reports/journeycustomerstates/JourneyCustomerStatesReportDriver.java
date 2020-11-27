package com.evolving.nglm.evolution.reports.journeycustomerstates;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

import java.util.Date;

public class JourneyCustomerStatesReportDriver extends ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(JourneyCustomerStatesReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {

    log.debug("Processing Journey Customer States Report with " + report + " and " + params);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String JOURNEY_ES_INDEX = "journeystatistic-";
    log.debug("PHASE 1 : read ElasticSearch");
    JourneyCustomerStatesReportMonoPhase.main(new String[] { elasticSearch, JOURNEY_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with Journey Customer States Report");

  }
}