/****************************************************************************
 *
 *  SubscriberReportMonoDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.subscriber;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class SubscriberReportMonoDriver extends ReportDriver{
	private static final Logger log = LoggerFactory.getLogger(SubscriberReportMonoDriver.class);

	@Override
	public void produceReport(
      Report report, 
      String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params) {
   log.debug("Processing Subscriber Report with "+report.getName());
   String esIndexSubscriber = "subscriberprofile";
    	
		SubscriberReportMonoPhase.main(new String[]{
		    kafka, elasticSearch, esIndexSubscriber, csvFilename
		});			
		
	  log.debug("Finished with Subscriber Report");
	}

	@Override
	public List<String> reportHeader() {
		// TODO Auto-generated method stub
		return null;
	}

}