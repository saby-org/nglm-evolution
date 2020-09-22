package com.evolving.nglm.evolution.reports.bdr;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class BDRReportMonoDriver extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(BDRReportMonoDriver.class);

  @Override
  public void produceReport(
      Report report,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params)
  {
    log.debug("Processing "+report.getName());
    String esIndexBdr = "detailedrecords_bonuses-";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BDRReportMonoPhase.main(new String[]{
        elasticSearch, esIndexBdr, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
    });         
    log.debug("Finished with BDR Report");
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
