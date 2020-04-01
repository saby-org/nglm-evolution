package com.evolving.nglm.evolution.datacubes.loyalty;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.ScheduledJob;

public class LoyaltyDatacubesOnTodayJob extends ScheduledJob
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
  *  LoyaltyProgramsHistory & LoyaltyProgramsChanges datacubeswill be generated every hours and will aggregate current data from today !
  *  Every hour will update the previous datacubes of the day, according to new data.
  *
  *****************************************/
  
  public LoyaltyDatacubesOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService) 
  {
    super(schedulingUniqueID, 
        "LoyaltyPrograms(today)", 
        Deployment.getTodayLoyaltyDatacubePeriodCronEntryString(), 
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
    this.loyaltyHistoryDatacube.run(now);
    this.tierChangesDatacube.run(now);
  }

}
