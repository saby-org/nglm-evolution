package com.evolving.nglm.evolution.datacubes.subscriber;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.SegmentationDimensionService;

public class SubscriberProfileDatacubesOnYesterdayJob extends ScheduledJob
{
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private SubscriberProfileDatacubeGenerator subscriberProfileDatacube;
  
  /*****************************************
  *
  * Constructor
  *  
  * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
  *
  *****************************************/
  public SubscriberProfileDatacubesOnYesterdayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService) 
  {
    super(schedulingUniqueID, 
        "SubscriberProfile(definitive)", 
        Deployment.getYesterdaySubscriberDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        false);
    this.subscriberProfileDatacube = new SubscriberProfileDatacubeGenerator(this.jobName, elasticsearch, segmentationDimensionService);
  }
  
  /*****************************************
  *
  * DatacubeScheduling
  *
  *****************************************/
  @Override
  protected void run()
  {
    this.subscriberProfileDatacube.definitive();
  }

}
