/****************************************************************************
 *
 *  SubscriberReportMonoDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.subscriber;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class SubscriberReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(SubscriberReportDriver.class);

  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing Subscriber Report with " + report.getName());
    String esIndexSubscriber = getSubscriberProfileIndex(reportGenerationDate);
    SubscriberReportMonoPhase.main(new String[] { kafka, elasticSearch, esIndexSubscriber, csvFilename }, reportGenerationDate);
    log.debug("Finished with Subscriber Report");
  }

}