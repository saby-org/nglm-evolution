package com.evolving.nglm.evolution.reports.notification;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class NotificationReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(NotificationReportDriver.class);
  public static final String ES_INDEX_NOTIFICATION_INITIAL = "detailedrecords_messages-";

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing " + report.getName());
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    NotificationReportMonoPhase.main(new String[] {elasticSearch, ES_INDEX_NOTIFICATION_INITIAL, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit }, reportGenerationDate);
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
