package com.evolving.nglm.evolution.datacubes.tiers;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;

public class TiersDatacubeOnTodayJob extends ScheduledJob
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
  *  This LoyaltyProgramsChanges datacube will be generated every hours and will aggregate current data from today !
  *  Every hour will update the previous datacube of the day, according to new data.
  *
  *****************************************/
  
  public TiersDatacubeOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, GUIManagerClient guiClient) 
  {
    super(schedulingUniqueID, 
        "Today-Tiers", 
        Deployment.getTodayTiersDatacubePeriodCronEntryString(), 
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
    this.datacube.run(now);
  }

}
