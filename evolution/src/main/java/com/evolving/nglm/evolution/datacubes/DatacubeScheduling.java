/*****************************************************************************
*
*  DatacubeScheduling.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.CronFormat;
import com.evolving.nglm.core.utilities.UtilitiesException;

public abstract class DatacubeScheduling implements Comparable<DatacubeScheduling>
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DatacubeScheduling.class);
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private long schedulingUniqueID;
  protected DatacubeGenerator datacube;
  private Date nextGenerationDate;
  private CronFormat periodicGeneration;
  protected boolean properlyConfigured;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DatacubeScheduling(long schedulingUniqueID, DatacubeGenerator datacube, Date nextGenerationDate, String periodicGenerationCronEntry, String baseTimeZone)
  {
    this.schedulingUniqueID = schedulingUniqueID;
    this.properlyConfigured = true;
    this.datacube = datacube;
    this.nextGenerationDate = nextGenerationDate;
    try
      {
        this.periodicGeneration = new CronFormat(periodicGenerationCronEntry, TimeZone.getTimeZone(baseTimeZone));
      } 
    catch (UtilitiesException e)
      {
        log.error("bad perodicEvaluationCronEntry {}", e.getMessage());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        
        this.properlyConfigured = false;
      }
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public Date getNextGenerationDate() { return this.nextGenerationDate; }
  public boolean isProperlyConfigured() { return this.properlyConfigured; }

  /*****************************************
  *
  *  absract
  *
  *****************************************/
  
  protected abstract void callDatacubeGenerator();
  
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
  public int compareTo(DatacubeScheduling o)
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

  public void generate() 
  {
    log.info("Start [" + this.datacube.getDatacubeName() + "] generation, scheduled for " + this.nextGenerationDate.toLocaleString());
    
    this.callDatacubeGenerator();
    
    this.nextGenerationDate = periodicGeneration.next();
    log.info("End [" + this.datacube.getDatacubeName() + "] generation with success, next generation is scheduled for " + this.nextGenerationDate.toLocaleString());
  }

  
  /*****************************************
  *
  *  toString
  *
  *****************************************/
  
  @Override
  public String toString() {
    return "{ID:" + this.schedulingUniqueID + ", " + this.datacube.datacubeName + ": " + this.getNextGenerationDate().toLocaleString() + "}";
  }
}
