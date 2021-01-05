package com.evolving.nglm.evolution.reports.bdr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import java.util.Date;

public class BDRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(BDRReportDriver.class);
  public static final String ES_INDEX_BDR_INITIAL = "detailedrecords_bonuses-";
  
  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BDRReportMonoPhase.main(new String[] { elasticSearch, ES_INDEX_BDR_INITIAL, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with BDR Report");
  }

}
