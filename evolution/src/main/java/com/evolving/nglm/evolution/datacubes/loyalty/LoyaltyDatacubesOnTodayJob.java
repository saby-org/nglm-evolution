package com.evolving.nglm.evolution.datacubes.loyalty;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class LoyaltyDatacubesOnTodayJob extends ScheduledJob
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
  * This will generate a datacube preview of the day from the subscriberprofile index (not a snapshot one).
  * Those data are not definitive, the day is not ended yet, metrics can still change.
  * 
  *****************************************/
  public LoyaltyDatacubesOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, LoyaltyProgramService loyaltyProgramService) 
  {
    super(schedulingUniqueID, 
        "LoyaltyPrograms(preview)", 
        Deployment.getTodayLoyaltyDatacubePeriodCronEntryString(), 
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
    this.loyaltyHistoryDatacube.preview();
    this.tierChangesDatacube.preview();
  }

}
