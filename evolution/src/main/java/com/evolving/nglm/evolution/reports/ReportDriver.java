/*****************************************************************************
 *
 *  ReportDriver.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution.reports;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Report;

/**
 * Abstract class that must be implemented to produce a report.
 *
 */
public abstract class ReportDriver {
	
	private static final Logger log = LoggerFactory.getLogger(ReportDriver.class);

	/**
	 * Produces a report (typically calls phase 1, 2 and 3 in sequence).
     * @param report report object
	 * @param zookeeper
	 * @param kafka
	 * @param elasticSearch
	 * @param csvFilename filename of report to produce
	 * @param params from reportmanager-run.sh
	 */
	abstract public void produceReport(
	        Report report, 
			String zookeeper, 
			String kafka, 
			String elasticSearch, 
			String csvFilename,
			String[] params);
	
	abstract public JSONArray reportFilters();
	
	/**
	 * Returns a topic name that can be used, based on the report being produced and the date.
	 * @param reportName
	 * @return
	 */
	public String getTopicPrefix(String reportName)
	{
   	String topic = ReportUtils.APPLICATION_ID_PREFIX + reportName + "_" + getTopicPrefixDate();
   	log.debug("topic : "+topic);
		return topic;
	}
	
  public String getTopicPrefixDate()
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

