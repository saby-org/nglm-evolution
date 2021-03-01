/*****************************************************************************
*
*  ScheduledJob.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.utilities.UtilitiesException;

public abstract class ScheduledJob implements Comparable<ScheduledJob>
{
  /*****************************************
  *
  *  static
  *
  *****************************************/
  protected static final Logger log = LoggerFactory.getLogger(ScheduledJob.class);
  
  private static Long currentUID = -1L;
  private static long getNextUID() {
    synchronized(currentUID)
    {
      currentUID = currentUID + 1;
      return currentUID;
    }
  }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/
  private long schedulingUniqueID;
  private Date nextGenerationDate;
  private CronFormat periodicGeneration;

  protected final String jobName;
  protected boolean properlyConfigured;

  /*****************************************
  *
  *  constructors
  *
  *****************************************/
  public ScheduledJob(String jobName, String periodicGenerationCronEntry, String baseTimeZone, boolean scheduleAtStart)
  {
    this.schedulingUniqueID = getNextUID();
    this.properlyConfigured = true;
    this.jobName = jobName;
    try
      {
        this.periodicGeneration = new CronFormat(periodicGenerationCronEntry, TimeZone.getTimeZone(baseTimeZone));
        this.nextGenerationDate = (scheduleAtStart) ? SystemTime.getCurrentTime() : this.periodicGeneration.next();
      } 
    catch (UtilitiesException e)
      {
        log.error("bad perodicEvaluationCronEntry {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());
        
        this.properlyConfigured = false;
      }
  }
  
  /**
   * Settings are retrieved directly from a ScheduledJobConfiguration
   */
  public ScheduledJob(ScheduledJobConfiguration settings)
  {
    this(settings.getJobID(), 
        settings.getCronEntry(),
        settings.getTimeZone(),
        settings.isScheduledAtRestart());
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  public Date getNextGenerationDate() { return this.nextGenerationDate; }
  public boolean isProperlyConfigured() { return this.properlyConfigured; }
  public long getSchedulingID() { return this.schedulingUniqueID; }

  /*****************************************
  *
  *  absract
  *
  *****************************************/
  protected abstract void run();
  
  /*****************************************
  *
  *  compareTo
  *  
  *  This function sort by generation date, but because
  *  two scheduling can have the same generation date, we 
  *  also provide an arbitrary order with schedulingUniqueID
  *  
  *  compareTo must return 0 if and only if both object
  *  are the exact same scheduling !
  *
  *****************************************/
  @Override
  public int compareTo(ScheduledJob o)
  {
    if(this.schedulingUniqueID == o.schedulingUniqueID)
      {
        return 0;
      }
    
    int result = nextGenerationDate.compareTo(o.getNextGenerationDate());
    if(result == 0) 
      {
        return (this.schedulingUniqueID > o.schedulingUniqueID)? 1: -1;
      } 
    else 
      {
        return result;
      }
    
  }
  
  /*****************************************
  *
  *  generate
  *
  *****************************************/
  public void call() 
  {
    log.info("Job-{" + this.jobName + "}: Start job scheduled for " + RLMDateUtils.formatDateForElasticsearchDefault(this.nextGenerationDate));
    
    this.run(); // TODO: Maybe add scheduled date later, if needed.
    this.nextGenerationDate = periodicGeneration.next();
    
    log.info("Job-{" + this.jobName + "}: Next call is scheduled for " + RLMDateUtils.formatDateForElasticsearchDefault(this.nextGenerationDate));
  }

  
  /*****************************************
  *
  *  toString
  *
  *****************************************/
  @Override
  public String toString() {
    return "{ID:" + this.schedulingUniqueID + ", " + this.jobName + ": " + RLMDateUtils.formatDateForElasticsearchDefault(this.nextGenerationDate)  + "}";
  }
}
