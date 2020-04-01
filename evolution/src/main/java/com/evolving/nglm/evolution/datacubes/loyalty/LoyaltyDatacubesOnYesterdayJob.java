package com.evolving.nglm.evolution.datacubes.loyalty;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.ScheduledJob;

public class LoyaltyDatacubesOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacube;
  private ProgramsChangesDatacubeGenerator tierChangesDatacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  LoyaltyProgramsHistory & LoyaltyProgramsChanges datacubes will be generated every day at 1:00 am
  *  and they will aggregate data from the previous day.
  *
  *****************************************/
  
  public LoyaltyDatacubesOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService) 
  {
    super(schedulingUniqueID, 
        "LoyaltyPrograms(yesterday)", 
        Deployment.getYesterdayLoyaltyDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.loyaltyHistoryDatacube = new ProgramsHistoryDatacubeGenerator(this.jobName+":History", elasticsearch, loyaltyProgramService);
    this.tierChangesDatacube = new ProgramsChangesDatacubeGenerator(this.jobName+":Changes", elasticsearch, loyaltyProgramService);
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
    this.loyaltyHistoryDatacube.run(yesterday);
    this.tierChangesDatacube.run(yesterday);
  }

}
