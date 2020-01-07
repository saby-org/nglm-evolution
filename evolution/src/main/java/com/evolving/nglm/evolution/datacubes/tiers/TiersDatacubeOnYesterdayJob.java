package com.evolving.nglm.evolution.datacubes.tiers;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;

public class TiersDatacubeOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private TiersDatacubeGenerator datacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This LoyaltyProgramsChanges datacube will be generated every day at 1:00 am
  *  and it will aggregate data from the previous day.
  *
  *****************************************/
  
  public TiersDatacubeOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, GUIManagerClient guiClient) 
  {
    super(schedulingUniqueID, 
        "Yesterday-Tiers", 
        Deployment.getYesterdayTiersDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.datacube = new TiersDatacubeGenerator(this.jobName, elasticsearch, guiClient);
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
