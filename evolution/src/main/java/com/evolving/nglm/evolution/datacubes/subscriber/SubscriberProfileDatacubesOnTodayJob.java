package com.evolving.nglm.evolution.datacubes.subscriber;

import java.util.Date;

import org.elasticsearch.client.RestHighLevelClient;

import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.ScheduledJob;

public class SubscriberProfileDatacubesOnTodayJob extends ScheduledJob
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private SubscriberProfileDatacubeGenerator subscriberProfileDatacube;
  
  /*****************************************
  *
  *  constructor
  *  
  *  This will generated a datacube every hours and will aggregate current data from the subscriberprofile index.
  *  Every hour will update the previous datacubes of the day, according to new data.
  *
  *****************************************/
  
  public SubscriberProfileDatacubesOnTodayJob(long schedulingUniqueID, RestHighLevelClient elasticsearch, SegmentationDimensionService segmentationDimensionService) 
  {
    super(schedulingUniqueID, 
        "SubscriberProfile(today)", 
        Deployment.getTodaySubscriberDatacubePeriodCronEntryString(), 
        Deployment.getBaseTimeZone(),
        true);
    this.subscriberProfileDatacube = new SubscriberProfileDatacubeGenerator(this.jobName, elasticsearch, segmentationDimensionService);
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
    this.subscriberProfileDatacube.run(now, true);
  }

}
