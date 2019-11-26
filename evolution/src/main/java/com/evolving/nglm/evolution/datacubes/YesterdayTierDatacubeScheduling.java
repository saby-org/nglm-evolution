package com.evolving.nglm.evolution.datacubes;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;

public class YesterdayTierDatacubeScheduling extends DatacubeScheduling
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  RestHighLevelClient elasticsearch;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This LoyaltyProgramsChanges datacube will be generated every day at 1:00 am
  *  and it will aggregate data from the previous day.
  *
  *****************************************/
  public YesterdayTierDatacubeScheduling(long schedulingUniqueID, RestHighLevelClient elasticsearch) 
  {
    super(schedulingUniqueID, new TierDatacubeGenerator("Yesterday-Tier"), SystemTime.getCurrentTime(), Deployment.getYesterdayTierDatacubePeriodCronEntryString(), Deployment.getBaseTimeZone());
    this.elasticsearch = elasticsearch;
  }
  

  /*****************************************
  *
  *  DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void callDatacubeGenerator()
  {
    Date now = SystemTime.getCurrentTime();
    Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
    this.datacube.run(yesterday, elasticsearch);
  }

}
