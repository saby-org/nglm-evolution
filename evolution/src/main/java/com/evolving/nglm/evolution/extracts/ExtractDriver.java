/****************************************************************************
 *
 *  SubscriberReportDriver.java 
 *
 ****************************************************************************/

package com.evolving.nglm.evolution.extracts;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion;
import com.evolving.nglm.evolution.Report;
import com.evolving.nglm.evolution.reports.ReportDriver;
import com.evolving.nglm.evolution.reports.ReportEsReader;
import com.evolving.nglm.evolution.reports.ReportUtils;
import com.evolving.nglm.evolution.reports.subscriber.SubscriberReportObjects;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.concurrent.TimeUnit;

/**
 * this class is driving reading from elastic search, put data in topic and write data to csv and compress data
 * extends ReportDriver in order to use the settings related to reports
 */
public class ExtractDriver extends ReportDriver
{
  private static final Logger log = LoggerFactory.getLogger(ExtractDriver.class);

  /**
   * @param extractItem   represent extractItem that will be processed @see ExtractItem
   * @param zookeeper     zookeper connection details
   * @param kafka         kafka connection details
   * @param elasticSearch elastic search connection details
   * @param csvFilename   name for csv file that will be generate
   * @throws Exception
   */
  public void produceExtract(ExtractItem extractItem, String zookeeper, String kafka, String elasticSearch, String csvFilename) throws Exception
  {
    try
    {
      //this part is based on report mechanism and reuse code from there

      log.debug("Processing extract " + extractItem.getExtractName());
      String topic = "Targets_" + extractItem.getExtractName() + "_" + getTopicPrefixDate(SystemTime.getCurrentTime());
      String esIndexSubscriber = "subscriberprofile";
      //String defaultReportPeriodUnit = target.getDefaultReportPeriodUnit();
      //int defaultReportPeriodQuantity = target.getDefaultReportPeriodQuantity();
      log.debug("PHASE 1 : read ElasticSearch");
      LinkedHashMap<String, QueryBuilder> esIndexWithQuery = new LinkedHashMap<String, QueryBuilder>();
      esIndexWithQuery.put(esIndexSubscriber, EvaluationCriterion.esCountMatchCriteriaGetQuery(extractItem.getEvaluationCriterionList()));

      ReportEsReader reportEsReader = new ReportEsReader(SubscriberReportObjects.KEY_STR, topic, kafka, zookeeper, elasticSearch, esIndexWithQuery, true, extractItem.getReturnNoOfRecords());
      reportEsReader.start();
      try
      {
        TimeUnit.SECONDS.sleep(1);
      }
      catch (InterruptedException e)
      {
      }

      log.debug("PHASE 2 : write csv file ");
      ExtractCsvWriter.main(new String[] { kafka, topic, csvFilename });
      ReportUtils.cleanupTopics(topic);
      log.debug("Finished with extract");
    }
    catch (Exception ex)
    {
      throw new Exception("Failed generate target ", ex);
    }
  }

  /**
   * Produces a report (typically calls phase 1, 2 and 3 in sequence).
   *
   * @param report        report object
   * @param zookeeper
   * @param kafka
   * @param elasticSearch
   * @param csvFilename   filename of report to produce
   * @param params        from reportmanager-run.sh
   */
  @Override public void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params)
  {
    //do nothing because this method will not be used
  }

}