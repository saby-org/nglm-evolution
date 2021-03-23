package com.evolving.nglm.evolution.reports.customerpointdetails;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class CustomerPointDetailsDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(CustomerPointDetailsDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing CustomerPointDetails Report with "+report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    String SUBSCRIBER_ES_INDEX = getSubscriberProfileIndex(reportGenerationDate);
    log.debug("data for report : " +SUBSCRIBER_ES_INDEX);
    log.debug("PHASE 1 : read ElasticSearch");
    log.trace(kafka+","+zookeeper+","+elasticSearch+","+SUBSCRIBER_ES_INDEX);
    CustomerPointDetailsMonoPhase.main(new String[]{elasticSearch, SUBSCRIBER_ES_INDEX, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit}, reportGenerationDate);     
    
    log.debug("Finished with CustomerPointDetails Report");
  }

}
