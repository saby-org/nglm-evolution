/****************************************************************************
 *
 *  TokenOfferReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.tokenOffer;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class TokenOfferReportDriver extends ReportDriver{
	private static final Logger log = LoggerFactory.getLogger(TokenOfferReportDriver.class);

	@Override
	public void produceReport(
            Report report, 
            final Date reportGenerationDate,
            String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params) {
    	log.debug("Processing Token Offer Report with "+report.getName());
    	
    	String esIndexSubscriber = getSubscriberProfileIndex(reportGenerationDate);
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();

      TokenOfferReportMonoPhase.main(new String[]{
          elasticSearch, esIndexSubscriber, csvFilename
      }, reportGenerationDate);     
  
	  log.debug("Finished with Token Report");
	}
}