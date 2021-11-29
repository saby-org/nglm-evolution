/****************************************************************************
 *
 *  BadgeCustomerReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.badgeCustomer;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.ReportDriver.ReportTypeDef;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;
import com.evolving.nglm.evolution.reports.tokenOffer.TokenOfferReportMonoPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

@ReportTypeDef(reportType = "detailedrecords")
public class BadgeCustomerReportDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(BadgeCustomerReportDriver.class);

  @Override
  public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID)
  {
    log.debug("Processing badgeCustomer Report with " + report.getName());

    String esIndexSubscriber = getSubscriberProfileIndex(reportGenerationDate);
    String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
    int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    BadgeCustomerReportMonoPhase.main(new String[] { elasticSearch, esIndexSubscriber, csvFilename, tenantID + "" }, reportGenerationDate);
    log.debug("Finished with badgeCustomer Report");
  }

  @Override public List<FilterObject> reportFilters()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override public List<String> reportHeader()
  {
    List<String> result = BadgeCustomerReportMonoPhase.headerFieldsOrder;
    return result;
  }
}