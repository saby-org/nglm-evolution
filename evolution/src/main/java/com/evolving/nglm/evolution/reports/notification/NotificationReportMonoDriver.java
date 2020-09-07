package com.evolving.nglm.evolution.reports.notification;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class NotificationReportMonoDriver extends ReportDriver{
  private static final Logger log = LoggerFactory.getLogger(NotificationReportMonoDriver.class);

  @Override
  public void produceReport(
      Report report,
      String zookeeper,
      String kafka,
      String elasticSearch,
      String csvFilename,
      String[] params) {
        log.debug("Processing "+report.getName());
        String esIndexNotif = "detailedrecords_messages-";
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
        
        NotificationReportMonoPhase.main(new String[]{
            elasticSearch, esIndexNotif, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit 
        });          
        log.debug("Finished with BDR Report");
  }

@Override
public List<FilterObject> reportFilters() {
	// TODO Auto-generated method stub
	return null;
}
}
