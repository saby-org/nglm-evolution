/****************************************************************************
 *
 *  VoucherCustomerReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.voucherCustomer;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class VoucherCustomerReportDriver extends ReportDriver{
	private static final Logger log = LoggerFactory.getLogger(VoucherCustomerReportDriver.class);

	@Override
	public void produceReport(
            Report report,
            final Date reportGenerationDate,
            String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params) {
    	log.debug("Processing voucherCustomer Report with "+report.getName());
    	
    	String esIndexSubscriber = getSubscriberProfileIndex(reportGenerationDate);
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();

      VoucherCustomerReportMonoPhase.main(new String[]{
          elasticSearch, esIndexSubscriber, csvFilename
      }, reportGenerationDate);        
  
	  log.debug("Finished with voucherCustomer Report");
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