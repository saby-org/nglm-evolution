/****************************************************************************
 *
 *  TokenReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.token;

import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportUtils;

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
    	String topic = super.getTopicPrefix(report.getName()) + "-a";
    	String esIndexSubscriber = "subscriberprofile";
        String defaultReportPeriodUnit = report.getDefaultReportPeriodUnit();
        int defaultReportPeriodQuantity = report.getDefaultReportPeriodQuantity();
    	log.debug("PHASE 1 : read ElasticSearch");
		TokenReportESReader.main(new String[]{
			topic, kafka, zookeeper, elasticSearch, esIndexSubscriber, String.valueOf(defaultReportPeriodQuantity), defaultReportPeriodUnit
		});			
		try { TimeUnit.SECONDS.sleep(1); } catch (InterruptedException e) {}
		
		log.debug("PHASE 2 : write csv file ");
		TokenReportCsvWriter.main(new String[]{
				kafka, topic, csvFilename
		});
    ReportUtils.cleanupTopics(topic);
	  log.debug("Finished with Token Report");
	}

}