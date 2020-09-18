package com.evolving.nglm.evolution.reports.odr;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.HeaderObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

public class ODRReportDriverOld extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ODRReportDriverOld.class);

  @Override
  public void produceReport(Report report, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    log.debug("Processing " + report.getName());
    String topicPrefix = super.getTopicPrefix(report.getName());
    String topic1 = topicPrefix;
    String esIndexOdr = "detailedrecords_offers-";
    String appIdPrefix = report.getName() + "_" + getTopicPrefixDate();

    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    
    log.debug("PHASE 1 : read ElasticSearch");
    ODRReportESReader.main(new String[] { topic1, kafka, zookeeper, elasticSearch, esIndexOdr, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit });
    try
      {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e)
      {
      }

    log.debug("PHASE 2 : write csv file ");
    ODRReportCsvWriter.main(new String[] { kafka, topic1, csvFilename });
    ReportUtils.cleanupTopics(topic1);
    log.debug("Finished with ODR Report");
  }

@Override
public List<HeaderObject> reportHeader() {
	// TODO Auto-generated method stub
	return null;
}
}
