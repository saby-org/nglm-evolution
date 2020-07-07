/****************************************************************************
 *
 *  SubscriberReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.Journey;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.subscriber.*;
import com.evolving.nglm.evolution.reports.ReportUtils;

import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

public class ExtractDriver
{
	private static final Logger log = LoggerFactory.getLogger(ExtractDriver.class);

	public void produceExtract(
            ExtractItem extractItem,
            String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename) throws Exception
	{
		try
			{
				//this part is based on report mechanism and reuse code from there

				log.debug("Processing extract " + extractItem.getExtractName());
				String topic = "Targets_" + extractItem.getExtractName() + "_" + getTopicPrefixDate();
				String esIndexSubscriber = "subscriberprofile";
				//String defaultReportPeriodUnit = target.getDefaultReportPeriodUnit();
				//int defaultReportPeriodQuantity = target.getDefaultReportPeriodQuantity();
				log.debug("PHASE 1 : read ElasticSearch");
				LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
				esIndexWithQuery.put(esIndexSubscriber, EvaluationCriterion.esCountMatchCriteriaGetQuery(extractItem.getEvaluationCriterionList()));

				ReportEsReader reportEsReader = new ReportEsReader(
								SubscriberReportObjects.KEY_STR,
								topic,
								kafka,
								zookeeper,
								elasticSearch,
								esIndexWithQuery,
								true,
								extractItem.getReturnNoOfRecords()
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
				ExtractCsvWriter.main(new String[]{
								kafka, topic, csvFilename
				});
				ReportUtils.cleanupTopics(topic);
				log.debug("Finished with extract");
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