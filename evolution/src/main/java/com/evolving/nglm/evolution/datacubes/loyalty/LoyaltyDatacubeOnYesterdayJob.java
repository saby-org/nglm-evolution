package com.evolving.nglm.evolution.datacubes.loyalty;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class LoyaltyDatacubeOnYesterdayJob extends ScheduledJob
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
  *  This LoyaltyProgramsHistory datacube will be generated every day at 1:00 am
  *  and it will aggregate data from the previous day.
  *
  *****************************************/
  
  public LoyaltyDatacubeOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch) 
  {
    super(schedulingUniqueID, 
        "Yesterday-Loyalty", 
        Deployment.getYesterdayLoyaltyDatacubePeriodCronEntryString(), 
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
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    this.datacube.run(yesterday);
  }

}
