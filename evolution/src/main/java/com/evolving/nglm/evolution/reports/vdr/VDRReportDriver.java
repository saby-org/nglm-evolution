package com.evolving.nglm.evolution.reports.vdr;

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

@ReportTypeDef(reportType = "detailedrecords")
public class VDRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(VDRReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing " + report.getName());
    String esIndexVDR = "detailedrecords_vouchers-";
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    VDRReportMonoPhase.main(new String[] { elasticSearch, esIndexVDR, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
    log.debug("Finished with VDR Report");
  }

  @Override
  public List<FilterObject> reportFilters()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> reportHeader()
  {
    // TODO Auto-generated method stub
    return null;
  }
}
