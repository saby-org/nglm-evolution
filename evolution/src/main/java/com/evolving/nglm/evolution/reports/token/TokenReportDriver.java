/****************************************************************************
 *
 *  TokenReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.token;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportMonoPhase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TokenReportDriver extends ReportDriver{
	private static final Logger log = LoggerFactory.getLogger(TokenReportDriver.class);

	@Override
	public void produceReport(
            Report report, 
            String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params) {
    	log.debug("Processing Token Report with "+report.getName());
    	
    	String esIndexSubscriber = "subscriberprofile";
      String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
      int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();

      TokenReportMonoPhase.main(new String[]{
          elasticSearch, esIndexSubscriber, csvFilename
      });     
  
	  log.debug("Finished with Token Report");
	}

}