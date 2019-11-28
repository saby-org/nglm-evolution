package com.evolving.nglm.evolution.datacubes.loyalty;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class LoyaltyDatacubeOnTodayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private LoyaltyDatacubeGenerator datacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This LoyaltyProgramsHistory datacube will be generated every hours and will aggregate current data from today !
  *  Every hour will update the previous datacube of the day, according to new data.
  *
  *****************************************/
  
  public LoyaltyDatacubeOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch) 
  {
    super(schedulingUniqueID, 
        "Today-Loyalty", 
        Deployment.getTodayLoyaltyDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.datacube = new LoyaltyDatacubeGenerator(this.jobName, elasticsearch);
  }

  /*****************************************
  *
  *  DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    Date now = SystemTime.getCurrentTime();
    this.datacube.run(now);
  }

}
