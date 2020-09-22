package com.evolving.nglm.evolution.reports.odr;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class ODRReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportDriver.class);

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing " + report.getName());
    String esIndexOdr = "detailedrecords_offers-";

    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    
    ODRReportMonoPhase.main(new String[] { elasticSearch, esIndexOdr, csvFilename, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit });
    log.debug("Finished with ODR Report");
  }

@Override
<<<<<<< HEAD
public List<FilterObject> reportFilters() {
=======
public List<String> reportHeader() {
>>>>>>> EVPRO-461
	// TODO Auto-generated method stub
	return null;
}
}
