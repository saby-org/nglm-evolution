/****************************************************************************
 *
 *  SubscriberReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.reports.extracts;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.Target;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.subscriber.*;
import com.evolving.nglm.evolution.reports.ReportUtils;

import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

public class TargetExtractDriver {
	private static final Logger log = LoggerFactory.getLogger(TargetExtractDriver.class);

	public void produceReport(
            Target target,
            String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params) throws Exception
	{
		try
			{
				log.debug("Processing Subscriber Report with " + target.getTargetName());
				String topic = "Targets_" + target.getTargetName() + "_" + getTopicPrefixDate();
				String esIndexSubscriber = "subscriberprofile";
				//String defaultReportPeriodUnit = target.getDefaultReportPeriodUnit();
				//int defaultReportPeriodQuantity = target.getDefaultReportPeriodQuantity();
				log.debug("PHASE 1 : read ElasticSearch");
				LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
				esIndexWithQuery.put(esIndexSubscriber, Journey.processEvaluateProfileCriteriaGetQuery(target.getTargetingCriteria()));

				ReportEsReader reportEsReader = new ReportEsReader(
								SubscriberReportObjects.KEY_STR,
								topic,
								kafka,
								zookeeper,
								elasticSearch,
								esIndexWithQuery,
								true
				);
				reportEsReader.start();
				try
					{
						TimeUnit.SECONDS.sleep(1);
					}
				catch (InterruptedException e)
					{
					}

				log.debug("PHASE 2 : write csv file ");
				TargetExtractCsvWriter.main(new String[]{
								kafka, topic, csvFilename
				});
				ReportUtils.cleanupTopics(topic);
				log.debug("Finished with Subscriber Report");
			}catch (Exception ex)
			{
				throw new Exception("Failed generate target ",ex);
			}
	}

	private String getTopicPrefixDate()
	{
		final String DateFormat = "yyyyMMdd_HHmmss";
		String topicSuffix = "";
		try
			{
				SimpleDateFormat sdf = new SimpleDateFormat(DateFormat);
				topicSuffix = sdf.format(SystemTime.getCurrentTime());
			}
		catch (IllegalArgumentException e)
			{
				log.error(DateFormat+" is invalid, using default "+e.getLocalizedMessage());
				topicSuffix = ""+System.currentTimeMillis();
			}
		return topicSuffix;
	}

}