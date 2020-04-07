package com.evolving.nglm.evolution.datacubes.subscriber;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class SubscriberProfileDatacubesOnTodayJob extends ScheduledJob
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
  * This will generated a datacube preview of the day from the subscriberprofile index (not a snapshot one).
  * Those data are not definitive, the day is not ended yet, metrics can still change.
  *
  *****************************************/
  public SubscriberProfileDatacubesOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService) 
  {
    super(schedulingUniqueID, 
        "SubscriberProfile(preview)", 
        Deployment.getTodaySubscriberDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
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
    this.subscriberProfileDatacube.preview();
  }

}
