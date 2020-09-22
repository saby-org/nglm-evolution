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
import com.evolving.nglm.evolution.reports.FilterObject;
import com.evolving.nglm.evolution.reports.ReportDriver;

public class SubscriberReportDriver extends ReportDriver{
	private static final Logger log = LoggerFactory.getLogger(SubscriberReportDriver.class);

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
<<<<<<< HEAD
	public List<FilterObject> reportFilters() {
=======
	public List<String> reportHeader() {
>>>>>>> EVPRO-461
		// TODO Auto-generated method stub
		return null;
	}

}