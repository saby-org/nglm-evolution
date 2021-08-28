package com.evolving.nglm.evolution.reports.bgdr;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.bdr.BDRReportMonoPhase;

@ReportTypeDef(reportType = "detailedrecords")
public class BGDRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(BGDRReportDriver.class);
  public static final String ES_INDEX_BGDR_INITIAL = "detailedrecords_badges-";

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BGDRReportMonoPhase.main(new String[] { elasticSearch, ES_INDEX_BGDR_INITIAL, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with BGDR Report");
  }

  @Override
  public List<FilterObject> reportFilters() {
	  return null;
  }

  @Override
  public List<String> reportHeader() {
    List<String> result = BGDRReportMonoPhase.headerFieldsOrder;
    return result;
  }
}
