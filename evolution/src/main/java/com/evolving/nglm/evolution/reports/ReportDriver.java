/*****************************************************************************
 *
 *  ReportDriver.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.Report;

/**
 * Abstract class that must be implemented to produce a report.
 *
 */
public abstract class ReportDriver
{

  private static final Logger log = LoggerFactory.getLogger(ReportDriver.class);
  private static final String SUBSCRIBERPROFILE_SNAPSHOT_INDEX_INITIAL = "subscriberprofile_snapshot-";
  private static final DateFormat DATE_FORMAT;
  static
  {
    DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
  }

  /**********************
   * 
   * produceReport
   * 
   **********************/
  
  public abstract void produceReport(Report report, final Date reportGenerationDate, String zookeeper, String kafka, String elasticSearch, String csvFilename, String[] params, int tenantID);

  /******************************
   * 
   * getTopicPrefix
   * 
   ******************************/
  
  public String getTopicPrefix(String reportName, final Date reportGenerationDate)
  {
    String topic = ReportUtils.APPLICATION_ID_PREFIX + reportName + "_" + getTopicPrefixDate(reportGenerationDate);
    log.debug("topic : " + topic);
    return topic;
  }

  /******************************
   * 
   * getTopicPrefixDate
   * 
   ******************************/
  
  public String getTopicPrefixDate(final Date reportGenerationDate)
  {
    final String DateFormat = "yyyyMMdd_HHmmss";
    String topicSuffix = "";
    try
      {
        SimpleDateFormat sdf = new SimpleDateFormat(DateFormat);
        topicSuffix = sdf.format(reportGenerationDate);
      } catch (IllegalArgumentException e)
      {
        log.error(DateFormat + " is invalid, using default " + e.getLocalizedMessage());
        topicSuffix = "" + System.currentTimeMillis();
      }
    return topicSuffix;
  }
  
  /******************************
   * 
   * getSubscriberProfileIndex
   * 
   ******************************/
  
  public String getSubscriberProfileIndex(final Date requestedDate)
  {
    if (0 == RLMDateUtils.truncatedCompareTo(requestedDate, SystemTime.getCurrentTime(), Calendar.DATE, Deployment.getBaseTimeZone()))
      {
        return "subscriberprofile";
      }
    else
      {
        return SUBSCRIBERPROFILE_SNAPSHOT_INDEX_INITIAL + DATE_FORMAT.format(requestedDate);
      }
  }

}
