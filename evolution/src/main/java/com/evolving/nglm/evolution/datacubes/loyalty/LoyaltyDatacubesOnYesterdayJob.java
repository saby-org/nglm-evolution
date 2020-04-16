package com.evolving.nglm.evolution.datacubes.loyalty;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.ScheduledJob;

public class LoyaltyDatacubesOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacube;
  private ProgramsChangesDatacubeGenerator tierChangesDatacube;
  
  /*****************************************
  *
  * Constructor
  * 
  * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
  * 
  *****************************************/
  public LoyaltyDatacubesOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService) 
  {
    super(schedulingUniqueID, 
        "LoyaltyPrograms(definitive)", 
        Deployment.getYesterdayLoyaltyDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.loyaltyHistoryDatacube = new ProgramsHistoryDatacubeGenerator(this.jobName+":History", elasticsearch, loyaltyProgramService);
    this.tierChangesDatacube = new ProgramsChangesDatacubeGenerator(this.jobName+":Changes", elasticsearch, loyaltyProgramService);
  }
  
  /*****************************************
  *
  * DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    this.loyaltyHistoryDatacube.definitive();
    this.tierChangesDatacube.definitive();
  }

}
