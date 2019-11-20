package com.evolving.nglm.evolution.datacubes;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;

public class TodayLoyaltyDatacubeScheduling extends DatacubeScheduling
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
  *  This LoyaltyProgramsHistory datacube will be generated every hours and will aggregate current data from today !
  *  Every hour will update the previous datacube of the day, according to new data.
  *
  *****************************************/
  public TodayLoyaltyDatacubeScheduling(long schedulingUniqueID, RestHighLevelClient elasticsearch) 
  {
    super(schedulingUniqueID, new LoyaltyDatacubeGenerator("Today-Loyalty"), SystemTime.getCurrentTime(), Deployment.getTodayLoyaltyDatacubePeriodCronEntryString(), Deployment.getBaseTimeZone());
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
    this.datacube.run(now, elasticsearch);
  }

}
